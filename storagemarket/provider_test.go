package storagemarket

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	mock_sealingpipeline "github.com/filecoin-project/boost/sealingpipeline/mock"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/smtestutil"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boost/transport/httptransport"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	piecestoreimpl "github.com/filecoin-project/go-fil-markets/piecestore/impl"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/shared_testutil"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	lapi "github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSimpleDealHappy(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// build the deal proposal with the blocking http test server and a completely blocking miner stub
	td := harness.newDealBuilder(t, 1).withAllMinerCallsBlocking().withBlockingHttpServer().build()

	// execute deal
	require.NoError(t, td.executeAndSubscribe())

	// wait for Accepted checkpoint
	td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)

	// unblock transfer -> wait for Transferred checkpoint and assert deals db and storage and fund manager
	td.unblockTransfer()
	td.waitForAndAssert(t, ctx, dealcheckpoints.Transferred)
	harness.AssertStorageAndFundManagerState(t, ctx, td.params.Transfer.Size, harness.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)

	// unblock publish -> wait for published checkpoint and assert
	td.unblockPublish()
	td.waitForAndAssert(t, ctx, dealcheckpoints.Published)
	harness.AssertStorageAndFundManagerState(t, ctx, td.params.Transfer.Size, harness.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)

	// unblock publish confirmation -> wait for publish confirmed and assert
	td.unblockWaitForPublish()
	td.waitForAndAssert(t, ctx, dealcheckpoints.PublishConfirmed)
	harness.EventuallyAssertStorageFundState(t, ctx, td.params.Transfer.Size, abi.NewTokenAmount(0), abi.NewTokenAmount(0))

	// unblock adding piece -> wait for piece to be added and assert
	td.unblockAddPiece()
	td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)
	harness.EventuallyAssertNoTagged(t, ctx)

	// expect Proving event to be fired
	err := td.waitForSealingState(lapi.SectorState(sealing.Proving))
	require.NoError(t, err)

	// assert logs
	lgs, err := harness.Provider.logsDB.Logs(ctx, td.params.DealUUID)
	require.NoError(t, err)
	require.NotEmpty(t, lgs)
}

func TestMultipleDealsConcurrent(t *testing.T) {
	nDeals := 10
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	tds := harness.executeNDealsConcurrentAndWaitFor(t, nDeals, func(i int) *testDeal {
		return harness.newDealBuilder(t, 1).withAllMinerCallsNonBlocking().withNormalHttpServer().build()
	}, func(_ int, td *testDeal) error {
		return td.waitForCheckpoint(dealcheckpoints.AddedPiece)
	})

	for i := 0; i < nDeals; i++ {
		td := tds[i]
		td.assertPieceAdded(t, ctx)
	}

	harness.EventuallyAssertNoTagged(t, ctx)
}

func TestMultipleDealsConcurrentWithFundsAndStorage(t *testing.T) {
	nDeals := 10
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	var errGrp errgroup.Group
	var tds []*testDeal
	totalStorage := uint64(0)
	totalCollat := abi.NewTokenAmount(0)
	totalPublish := abi.NewTokenAmount(0)
	// half the deals will finish, half will be blocked on the wait for publish call -> we will then assert that the funds and storage manager state is as expected
	for i := 0; i < nDeals; i++ {
		i := i
		var td *testDeal
		// for even numbered deals, we will never block
		if i%2 == 0 {
			// setup mock publish & add-piece expectations with non-blocking behaviours -> the associated tagged funds and storage will be released
			td = harness.newDealBuilder(t, i).withAllMinerCallsNonBlocking().withNormalHttpServer().build()
		} else {
			// for odd numbered deals, we will block on the publish-confirm step
			// setup mock publish & add-piece expectations with blocking wait-for-publish behaviours -> the associated tagged funds and storage will not be released
			td = harness.newDealBuilder(t, i).withPublishNonBlocking().withPublishConfirmBlocking().withAddPieceBlocking().withNormalHttpServer().build()
			totalStorage = totalStorage + td.params.Transfer.Size
			totalCollat = abi.NewTokenAmount(totalCollat.Add(totalCollat.Int, td.params.ClientDealProposal.Proposal.ProviderCollateral.Int).Int64())
			totalPublish = abi.NewTokenAmount(totalPublish.Add(totalPublish.Int, harness.MinPublishFees.Int).Int64())
		}

		tds = append(tds, td)

		errGrp.Go(func() error {
			err := td.executeAndSubscribe()
			if err != nil {
				return err
			}
			var checkpoint dealcheckpoints.Checkpoint
			if i%2 == 0 {
				checkpoint = dealcheckpoints.AddedPiece
			} else {
				checkpoint = dealcheckpoints.Published
			}
			if err := td.waitForCheckpoint(checkpoint); err != nil {
				return err
			}

			return nil
		})
	}
	require.NoError(t, errGrp.Wait())

	for i := 0; i < nDeals; i++ {
		td := tds[i]
		if i%2 == 0 {
			td.assertPieceAdded(t, ctx)
		} else {
			td.assertDealPublished(t, ctx)
		}
	}

	harness.EventuallyAssertStorageFundState(t, ctx, totalStorage, totalPublish, totalCollat)

	// now confirm the publish for remaining deals and assert funds and storage
	for i := 0; i < nDeals; i++ {
		td := tds[i]
		if i%2 != 0 {
			td.unblockWaitForPublish()
			totalPublish = abi.NewTokenAmount(totalPublish.Sub(totalPublish.Int, harness.MinPublishFees.Int).Int64())
			totalCollat = abi.NewTokenAmount(totalCollat.Sub(totalCollat.Int, td.params.ClientDealProposal.Proposal.ProviderCollateral.Int).Int64())
		}
	}
	harness.EventuallyAssertStorageFundState(t, ctx, totalStorage, totalPublish, totalCollat)

	// now finish the remaining deals and assert funds and storage
	for i := 0; i < nDeals; i++ {
		td := tds[i]
		if i%2 != 0 {
			td.unblockAddPiece()
			totalStorage = totalStorage - td.params.Transfer.Size
		}
	}
	harness.EventuallyAssertNoTagged(t, ctx)
	// assert that piece has been added for the deals
	for i := 0; i < nDeals; i++ {
		if i%2 != 0 {
			td := tds[i]
			td.assertPieceAdded(t, ctx)
		}
	}
}

func TestDealsRejectedForFunds(t *testing.T) {
	ctx := context.Background()
	// setup the provider test harness with configured publish fee per deal and a total wallet balance.
	harness := NewHarness(t, ctx, withMinPublishFees(abi.NewTokenAmount(100)), withPublishWalletBal(1000))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// 10 deals should get accepted and 5 deals should fail as we wont have enough funds to pay for the publishing costs.
	nDeals := 15
	var errg errgroup.Group

	var mu sync.Mutex
	var failedTds []*testDeal
	var successTds []*testDeal

	for i := 0; i < nDeals; i++ {
		td := harness.newDealBuilder(t, i).withNoOpMinerStub().withBlockingHttpServer().build()
		errg.Go(func() error {
			if err := td.executeAndSubscribe(); err != nil {
				// deal should be rejected only for lack of funds
				if !strings.Contains(err.Error(), "insufficient funds") {
					return errors.New("did not get expected error")
				}

				mu.Lock()
				failedTds = append(failedTds, td)
				mu.Unlock()
			} else {
				mu.Lock()
				successTds = append(successTds, td)
				mu.Unlock()
			}

			return nil
		})
	}
	require.NoError(t, errg.Wait())
	// ensure 10 deals got accepted and five deals failed
	require.Len(t, successTds, 10)
	require.Len(t, failedTds, 5)

	// cancel all transfers so all deals finish and db files can be deleted
	for i := range successTds {
		td := successTds[i]
		require.NoError(t, harness.Provider.CancelDealDataTransfer(td.params.DealUUID))
		td.assertEventuallyDealCleanedup(t, ctx)
	}

}

func TestDealRejectedForDuplicateProposal(t *testing.T) {
	ctx := context.Background()
	harness := NewHarness(t, ctx)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	t.Run("online", func(t *testing.T) {
		td := harness.newDealBuilder(t, 1).withNoOpMinerStub().withBlockingHttpServer().build()
		err := td.executeAndSubscribe()
		require.NoError(t, err)

		pi, _, err := td.ph.Provider.ExecuteDeal(td.params, "")
		require.NoError(t, err)
		require.False(t, pi.Accepted)
		require.Contains(t, pi.Reason, "deal proposal is identical")
	})

	t.Run("offline", func(t *testing.T) {
		td := harness.newDealBuilder(t, 1, withOfflineDeal()).withNoOpMinerStub().build()
		_, _, err := td.ph.Provider.ExecuteDeal(td.params, "")
		require.NoError(t, err)

		pi, _, err := td.ph.Provider.ExecuteDeal(td.params, "")
		require.NoError(t, err)
		require.False(t, pi.Accepted)
		require.Contains(t, pi.Reason, "deal proposal is identical")
	})
}

func TestDealRejectedForDuplicateUuid(t *testing.T) {
	ctx := context.Background()
	harness := NewHarness(t, ctx)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	t.Run("online", func(t *testing.T) {
		td := harness.newDealBuilder(t, 1).withNoOpMinerStub().withBlockingHttpServer().build()
		err := td.executeAndSubscribe()
		require.NoError(t, err)

		td2 := harness.newDealBuilder(t, 2).withNoOpMinerStub().withBlockingHttpServer().build()
		td2.params.DealUUID = td.params.DealUUID
		pi, _, err := td.ph.Provider.ExecuteDeal(td2.params, "")
		require.NoError(t, err)
		require.False(t, pi.Accepted)
		require.Contains(t, pi.Reason, "deal has the same uuid")
	})

	t.Run("offline", func(t *testing.T) {
		td := harness.newDealBuilder(t, 1, withOfflineDeal()).withNoOpMinerStub().build()
		_, _, err := td.ph.Provider.ExecuteDeal(td.params, "")
		require.NoError(t, err)

		td2 := harness.newDealBuilder(t, 2, withOfflineDeal()).withNoOpMinerStub().build()
		td2.params.DealUUID = td.params.DealUUID
		pi, _, err := td.ph.Provider.ExecuteDeal(td2.params, "")
		require.NoError(t, err)
		require.False(t, pi.Accepted)
		require.Contains(t, pi.Reason, "deal has the same uuid")
	})
}

func TestDealRejectedForInsufficientProviderFunds(t *testing.T) {
	ctx := context.Background()
	// setup the provider test harness with configured publish fee per deal
	// that is more than the total wallet balance.
	harness := NewHarness(t, ctx, withMinPublishFees(abi.NewTokenAmount(100)), withPublishWalletBal(50))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	td := harness.newDealBuilder(t, 1).withNoOpMinerStub().withBlockingHttpServer().build()
	pi, _, err := td.ph.Provider.ExecuteDeal(td.params, peer.ID(""))
	require.NoError(t, err)
	require.False(t, pi.Accepted)
	require.Contains(t, pi.Reason, "insufficient funds")
}

func TestDealRejectedForInsufficientProviderStorageSpace(t *testing.T) {
	ctx := context.Background()
	// setup the provider test harness with only 1 byte of storage
	// space for incoming deals.
	harness := NewHarness(t, ctx, withMaxStagingDealsBytes(1))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	td := harness.newDealBuilder(t, 1).withNoOpMinerStub().withBlockingHttpServer().build()
	pi, _, err := td.ph.Provider.ExecuteDeal(td.params, peer.ID(""))
	require.NoError(t, err)
	require.False(t, pi.Accepted)
	require.Contains(t, pi.Reason, "no space left")
}

func TestDealFailuresHandlingNonRecoverableErrors(t *testing.T) {
	require.NoError(t, logging.SetLogLevel("*", "INFO"))

	ctx := context.Background()
	// setup the provider test harness with a disconnecting server that disconnects after sending the given number of bytes
	harness := NewHarness(t, ctx, withHttpDisconnectServerAfter(1),
		withHttpTransportOpts([]httptransport.Option{httptransport.BackOffRetryOpt(1*time.Millisecond, 1*time.Millisecond, 2, 1)}))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// spin up four deals
	// deal 1 -> fails transfer, deal 2 -> fails publish, deal 3 -> fails publish confirm, deal 4 -> fails add piece
	publishErr := errors.New("publish failed")
	publishConfirmErr := errors.New("publish confirm error")
	addPieceErr := errors.New("add piece error")
	deals := []struct {
		dealBuilder func() *testDeal
		errContains string
	}{
		{
			dealBuilder: func() *testDeal {
				return harness.newDealBuilder(t, 1).withFailingHttpServer().build()
			},
			errContains: "failed data transfer",
		},
		{
			dealBuilder: func() *testDeal {
				return harness.newDealBuilder(t, 1).withPublishFailing(publishErr).withNormalHttpServer().build()
			},
			errContains: publishErr.Error(),
		},
		{
			dealBuilder: func() *testDeal {
				return harness.newDealBuilder(t, 1).withPublishNonBlocking().withPublishConfirmFailing(publishConfirmErr).withNormalHttpServer().build()
			},
			errContains: publishConfirmErr.Error(),
		},
		{
			dealBuilder: func() *testDeal {
				return harness.newDealBuilder(t, 1).withPublishNonBlocking().
					withPublishConfirmNonBlocking().withAddPieceFailing(addPieceErr).withNormalHttpServer().build()
			},
			errContains: addPieceErr.Error(),
		},
	}

	tds := harness.executeNDealsConcurrentAndWaitFor(t, len(deals), func(i int) *testDeal {
		return deals[i].dealBuilder()
	}, func(i int, td *testDeal) error {
		return td.waitForError(deals[i].errContains)
	})

	// assert cleanup of deal and db state
	for i := range tds {
		td := tds[i]
		derr := deals[i].errContains
		td.assertEventuallyDealCleanedup(t, ctx)
		td.assertDealFailedNonRecoverable(t, ctx, derr)
	}

	// assert storage manager and funds
	harness.EventuallyAssertNoTagged(t, ctx)
}

func TestDealAskValidation(t *testing.T) {
	ctx := context.Background()

	tcs := map[string]struct {
		ask         *storagemarket.StorageAsk
		dbuilder    func(h *ProviderHarness) *testDeal
		expectedErr string
	}{
		"fails if price below minimum for unverified deal": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(100000000000),
			},
			dbuilder: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withNoOpMinerStub().build()

			},
			expectedErr: "storage price per epoch less than asking price",
		},
		"fails if price below minimum for verified deal": {
			ask: &storagemarket.StorageAsk{
				Price:         abi.NewTokenAmount(0),
				VerifiedPrice: abi.NewTokenAmount(100000000000),
			},
			dbuilder: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withVerifiedDeal()).withNoOpMinerStub().build()

			},
			expectedErr: "storage price per epoch less than asking price",
		},
		"fails if piece size below minimum": {
			ask: &storagemarket.StorageAsk{
				Price:        abi.NewTokenAmount(0),
				MinPieceSize: abi.PaddedPieceSize(1000000000),
			},
			dbuilder: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withNormalFileSize(100)).withNoOpMinerStub().build()

			},
			expectedErr: "piece size less than minimum required size",
		},
		"fails if piece size above maximum": {
			ask: &storagemarket.StorageAsk{
				Price:        abi.NewTokenAmount(0),
				MaxPieceSize: abi.PaddedPieceSize(1),
			},
			dbuilder: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withNormalFileSize(100)).withNoOpMinerStub().build()

			},
			expectedErr: "piece size more than maximum allowed size",
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t, ctx, withStoredAsk(tc.ask.Price, tc.ask.VerifiedPrice, tc.ask.MinPieceSize, tc.ask.MaxPieceSize))
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal with the blocking http test server and a completely blocking miner stub
			td := tc.dbuilder(harness)

			// execute deal
			err := td.executeAndSubscribe()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func TestDealVerification(t *testing.T) {
	ctx := context.Background()

	tcs := map[string]struct {
		ask         *storagemarket.StorageAsk
		dbuilder    func(t *testing.T, h *ProviderHarness) *testDeal
		expectedErr string
		expect      func(h *ProviderHarness)
		opts        []harnessOpt
	}{
		"fails if client does not have enough datacap for verified deal": {
			ask: &storagemarket.StorageAsk{
				VerifiedPrice: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withVerifiedDeal()).withNoOpMinerStub().build()
			},
			expect: func(h *ProviderHarness) {
				sp := abi.NewStoragePower(1)
				h.MockFullNode.EXPECT().StateVerifiedClientStatus(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sp,
					nil)
			},
			expectedErr: "verified deal DataCap 1 too small",
		},
		"fails if can't fetch datacap for verified deal": {
			ask: &storagemarket.StorageAsk{
				VerifiedPrice: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withVerifiedDeal()).withNoOpMinerStub().build()
			},
			expect: func(h *ProviderHarness) {
				h.MockFullNode.EXPECT().StateVerifiedClientStatus(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					errors.New("some error"))
			},
			expectedErr: "getting verified datacap",
		},
		"fails if client does NOT have enough balance for deal": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withNoOpMinerStub().build()
			},
			opts:        []harnessOpt{withStateMarketBalance(abi.NewTokenAmount(10), abi.NewTokenAmount(10))},
			expectedErr: "funds in escrow 0 not enough",
		},
		"fails if client signature is not valid": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withNoOpMinerStub().build()
			},
			expect: func(h *ProviderHarness) {
				h.Provider.sigVerifier = &mockSignatureVerifier{false, nil}
			},
			expectedErr: "invalid signature",
		},
		"fails if client signature verification fails": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withNoOpMinerStub().build()
			},
			expect: func(h *ProviderHarness) {
				h.Provider.sigVerifier = &mockSignatureVerifier{true, errors.New("some error")}
			},
			expectedErr: "validating signature",
		},
		"fails if proposed provider collateral below minimum": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withProviderCollateral(abi.NewTokenAmount(0))).withNoOpMinerStub().build()
			},
			expectedErr: "proposed provider collateral 0 below minimum",
		},
		"fails if proposed provider collateral above maximum": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withProviderCollateral(abi.NewTokenAmount(100))).withNoOpMinerStub().build()
			},
			expectedErr: "proposed provider collateral 100 above maximum",
		},

		"fails if provider address does not match": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				addr, err := address.NewIDAddress(1)
				require.NoError(t, err)
				return h.newDealBuilder(t, 1, withMinerAddr(addr)).withNoOpMinerStub().build()
			},
			expectedErr: "incorrect provider for deal",
		},
		"proposal piece cid has wrong prefix": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withPieceCid(testutil.GenerateCid())).withNoOpMinerStub().build()
			},
			expectedErr: "proposal PieceCID had wrong prefix",
		},
		"proposal piece cid undefined": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withUndefinedPieceCid()).withNoOpMinerStub().build()
			},
			expectedErr: "proposal PieceCID undefined",
		},
		"proposal end 9 before proposal start 10": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withEpochs(abi.ChainEpoch(10), abi.ChainEpoch(9))).withNoOpMinerStub().build()
			},
			expectedErr: "proposal end 9 before proposal start 10",
		},
		"deal start epoch has already elapsed": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withEpochs(abi.ChainEpoch(-1), abi.ChainEpoch(9))).withNoOpMinerStub().build()
			},
			expectedErr: "deal start epoch -1 has already elapsed",
		},
		"deal label greater than max size": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				label := strings.Repeat("a", 1000)
				return h.newDealBuilder(t, 1, withLabel(label)).withNoOpMinerStub().build()
			},
			expectedErr: "deal label can be at most 256 bytes",
		},
		"deal piece size invalid": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withPieceSize(abi.PaddedPieceSize(1000))).withNoOpMinerStub().build()
			},
			expectedErr: "proposal piece size is invalid",
		},
		"deal end epoch too far out": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				start := miner.MaxSectorExpirationExtension - market2.DealMinDuration - 1
				maxEndEpoch := miner.MaxSectorExpirationExtension + 100
				return h.newDealBuilder(t, 1, withEpochs(abi.ChainEpoch(start), abi.ChainEpoch(maxEndEpoch))).withNoOpMinerStub().build()
			},
			expectedErr: "invalid deal end epoch",
		},
		"deal duration greater than max duration": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {

				return h.newDealBuilder(t, 1, withEpochs(0, abi.ChainEpoch(market2.DealMaxDuration+1))).withNoOpMinerStub().build()
			},
			expectedErr: "deal duration out of bounds",
		},
		"deal duration less than min duration": {
			ask: &storagemarket.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {

				return h.newDealBuilder(t, 1, withEpochs(1, 2)).withNoOpMinerStub().build()
			},
			expectedErr: "deal duration out of bounds",
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t, ctx, tc.opts...)
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal with the blocking http test server and a completely blocking miner stub
			td := tc.dbuilder(t, harness)
			if tc.expect != nil {
				tc.expect(harness)
			}

			// execute deal
			err := td.executeAndSubscribe()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func (h *ProviderHarness) executeNDealsConcurrentAndWaitFor(t *testing.T, nDeals int,
	buildDeal func(i int) *testDeal, waitF func(i int, td *testDeal) error) []*testDeal {
	tds := make([]*testDeal, 0, nDeals)
	var errG errgroup.Group
	for i := 0; i < nDeals; i++ {
		i := i
		// build the deal proposal
		td := buildDeal(i)
		tds = append(tds, td)

		errG.Go(func() error {
			err := td.executeAndSubscribe()
			if err != nil {
				return err
			}
			if err := waitF(i, td); err != nil {
				return err
			}
			return nil
		})
	}

	require.NoError(t, errG.Wait())

	return tds
}

func (h *ProviderHarness) AssertAccepted(t *testing.T, ctx context.Context, dp *types.DealParams) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), nil, dealcheckpoints.Accepted, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), "")
}

func (h *ProviderHarness) AssertTransferred(t *testing.T, ctx context.Context, dp *types.DealParams) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), nil, dealcheckpoints.Transferred, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), "")
}

func (h *ProviderHarness) AssertPublished(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), &so.PublishCid, dealcheckpoints.Published, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), "")
}

func (h *ProviderHarness) AssertDealFailedTransferNonRecoverable(t *testing.T, ctx context.Context, dp *types.DealParams, errStr string) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), nil, dealcheckpoints.Complete, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), errStr)
}

func (h *ProviderHarness) AssertPublishConfirmed(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput) {
	h.AssertDealDBState(t, ctx, dp, so.DealID, &so.FinalPublishCid, dealcheckpoints.PublishConfirmed, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), "")
}

func (h *ProviderHarness) AssertPieceAdded(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput, carv2FilePath string) {
	h.AssertEventuallyDealCleanedup(t, ctx, dp)
	h.AssertDealDBState(t, ctx, dp, so.DealID, &so.FinalPublishCid, dealcheckpoints.IndexedAndAnnounced, so.SectorID, so.Offset, dp.ClientDealProposal.Proposal.PieceSize.Unpadded().Padded(), "")
	// Assert that the original file data we sent matches what was sent to the sealer
	h.AssertSealedContents(t, carv2FilePath, *so.SealedBytes)
	// assert that dagstore and piecestore have this deal
	dbState, err := h.DealsDB.ByID(ctx, dp.DealUUID)
	require.NoError(t, err)
	rg, ok := h.DAGStore.GetRegistration(dbState.ClientDealProposal.Proposal.PieceCID)
	require.True(t, ok)
	require.True(t, rg.EagerInit)
	require.Empty(t, rg.CarPath)
}

func (h *ProviderHarness) EventuallyAssertNoTagged(t *testing.T, ctx context.Context) {
	h.EventuallyAssertStorageFundState(t, ctx, 0, abi.NewTokenAmount(0), abi.NewTokenAmount(0))
}

func (h *ProviderHarness) EventuallyAssertStorageFundState(t *testing.T, ctx context.Context, taggedStorage uint64, pub, collat abi.TokenAmount) {
	require.Eventually(t, func() bool {
		st, _ := h.StorageDB.TotalTagged(ctx)
		if st != taggedStorage {
			return false
		}
		ft, _ := h.FundsDB.TotalTagged(ctx)

		return ft.PubMsg.Uint64() == pub.Uint64() && ft.Collateral.Uint64() == collat.Uint64()
	}, 5*time.Second, 100*time.Millisecond)
}

func (h *ProviderHarness) AssertStorageAndFundManagerState(t *testing.T, ctx context.Context, taggedStorage uint64, pub, collat abi.TokenAmount) {
	h.AssertStorageManagerState(t, ctx, taggedStorage)
	h.AssertFundManagerState(t, ctx, pub, collat)
}

func (h *ProviderHarness) AssertStorageManagerState(t *testing.T, ctx context.Context, taggedStorage uint64) {
	st, err := h.StorageDB.TotalTagged(ctx)
	require.NoError(t, err)
	require.EqualValues(t, taggedStorage, st)
}

func (h *ProviderHarness) AssertFundManagerState(t *testing.T, ctx context.Context, pub, collat abi.TokenAmount) {
	ft, err := h.FundsDB.TotalTagged(ctx)
	require.NoError(t, err)
	require.EqualValues(t, pub, ft.PubMsg)
	require.EqualValues(t, collat, ft.Collateral)
}

func (h *ProviderHarness) AssertSealedContents(t *testing.T, carV2FilePath string, read []byte) {
	cr, err := carv2.OpenReader(carV2FilePath)
	require.NoError(t, err)
	defer cr.Close()

	actual, err := ioutil.ReadAll(cr.DataReader())
	require.NoError(t, err)

	// the read-bytes also contains extra zeros for the padding magic, so just match without the padding bytes.
	require.EqualValues(t, actual, read[:len(actual)])
}

func (h *ProviderHarness) AssertEventuallyDealCleanedup(t *testing.T, ctx context.Context, dp *types.DealParams) {
	dbState, err := h.DealsDB.ByID(ctx, dp.DealUUID)
	require.NoError(t, err)
	// assert that the deal has been cleanedup and there are no leaks
	require.Eventually(t, func() bool {
		// deal handler should be deleted
		dh := h.Provider.getDealHandler(dbState.DealUuid)
		if dh != nil {
			return false
		}

		// the deal inbound file should no longer exist if it is an online deal
		if !dp.IsOffline {
			_, statErr := os.Stat(dbState.InboundFilePath)
			return statErr != nil
		}
		return true
	}, 5*time.Second, 200*time.Millisecond)
}

func (h *ProviderHarness) AssertDealDBState(t *testing.T, ctx context.Context, dp *types.DealParams, expectedDealID abi.DealID, publishCid *cid.Cid,
	checkpoint dealcheckpoints.Checkpoint, sector abi.SectorNumber, offset, length abi.PaddedPieceSize, errStr string) {
	dbState, err := h.DealsDB.ByID(ctx, dp.DealUUID)
	require.NoError(t, err)
	require.EqualValues(t, dp.DealUUID, dbState.DealUuid)
	require.EqualValues(t, dp.DealDataRoot, dbState.DealDataRoot)
	require.EqualValues(t, expectedDealID, dbState.ChainDealID)
	require.EqualValues(t, checkpoint, dbState.Checkpoint)
	require.EqualValues(t, sector, dbState.SectorID)
	require.EqualValues(t, offset, dbState.Offset)
	require.EqualValues(t, length, dbState.Length)
	require.EqualValues(t, dp.Transfer, dbState.Transfer)

	if len(errStr) == 0 {
		require.Empty(t, dbState.Err)
	} else {
		require.Contains(t, dbState.Err, errStr)
	}

	if publishCid == nil {
		require.Empty(t, dbState.PublishCID)
	} else {
		require.EqualValues(t, publishCid, dbState.PublishCID)
	}
}

type ProviderHarness struct {
	Host                   host.Host
	GoMockCtrl             *gomock.Controller
	TempDir                string
	MinerAddr              address.Address
	ClientAddr             address.Address
	MockFullNode           *lotusmocks.MockFullNode
	MinerStub              *smtestutil.MinerStub
	DealsDB                *db.DealsDB
	FundsDB                *db.FundsDB
	StorageDB              *db.StorageDB
	PublishWallet          address.Address
	MinPublishFees         abi.TokenAmount
	MaxStagingDealBytes    uint64
	MockSealingPipelineAPI *mock_sealingpipeline.MockAPI

	Provider *Provider

	// http test servers
	NormalServer        *httptest.Server
	BlockingServer      *testutil.BlockingHttpTestServer
	DisconnectingServer *httptest.Server
	FailingServer       *httptest.Server

	SqlDB    *sql.DB
	DAGStore *shared_testutil.MockDagStoreWrapper
}

type providerConfig struct {
	maxStagingDealBytes  uint64
	minPublishFees       abi.TokenAmount
	disconnectAfterEvery int64
	httpOpts             []httptransport.Option

	lockedFunds      big.Int
	escrowFunds      big.Int
	publishWalletBal int64

	price         abi.TokenAmount
	verifiedPrice abi.TokenAmount
	minPieceSize  abi.PaddedPieceSize
	maxPieceSize  abi.PaddedPieceSize
}

type harnessOpt func(pc *providerConfig)

// withHttpTransportOpts configures the http transport config for the provider
func withHttpTransportOpts(opts []httptransport.Option) harnessOpt {
	return func(pc *providerConfig) {
		pc.httpOpts = opts
	}
}

// withHttpDisconnectServerAfter configures the disconnecting server of the harness to disconnect after sending `after` bytes.
// TODO: This should be per-deal rather than at the harness level
func withHttpDisconnectServerAfter(afterEvery int64) harnessOpt {
	return func(pc *providerConfig) {
		pc.disconnectAfterEvery = afterEvery
	}
}

func withMinPublishFees(fee abi.TokenAmount) harnessOpt {
	return func(pc *providerConfig) {
		pc.minPublishFees = fee
	}
}

func withPublishWalletBal(bal int64) harnessOpt {
	return func(pc *providerConfig) {
		pc.publishWalletBal = bal
	}
}

func withMaxStagingDealsBytes(max uint64) harnessOpt {
	return func(pc *providerConfig) {
		pc.maxStagingDealBytes = max
	}
}

func withStoredAsk(price, verifiedPrice abi.TokenAmount, minPieceSize, maxPieceSize abi.PaddedPieceSize) harnessOpt {
	return func(pc *providerConfig) {
		pc.price = price
		pc.verifiedPrice = verifiedPrice
		pc.minPieceSize = minPieceSize
		pc.maxPieceSize = maxPieceSize
	}
}

func withStateMarketBalance(locked, escrow abi.TokenAmount) harnessOpt {
	return func(pc *providerConfig) {
		pc.lockedFunds = locked
		pc.escrowFunds = escrow
	}
}

func NewHarness(t *testing.T, ctx context.Context, opts ...harnessOpt) *ProviderHarness {
	pc := &providerConfig{
		minPublishFees:       abi.NewTokenAmount(100),
		maxStagingDealBytes:  10000000000,
		disconnectAfterEvery: 1048600,
		lockedFunds:          big.NewInt(3000000),
		escrowFunds:          big.NewInt(5000000),
		publishWalletBal:     1000,

		price:         abi.NewTokenAmount(0),
		verifiedPrice: abi.NewTokenAmount(0),
		minPieceSize:  abi.PaddedPieceSize(0),
		maxPieceSize:  abi.PaddedPieceSize(10737418240), //10Gib default
	}

	sealingpipelineStatus := map[lapi.SectorState]int{
		"AddPiece":       0,
		"Packing":        0,
		"PreCommit1":     1,
		"PreCommit2":     0,
		"PreCommitWait":  0,
		"WaitSeed":       1,
		"Committing":     0,
		"CommitWait":     0,
		"FinalizeSector": 0,
	}

	for _, opt := range opts {
		opt(pc)
	}
	// Create a temporary directory for all the tests.
	dir := t.TempDir()

	// setup mocks
	ctrl := gomock.NewController(t)
	fn := lotusmocks.NewMockFullNode(ctrl)
	minerStub := smtestutil.NewMinerStub(ctrl)
	sps := mock_sealingpipeline.NewMockAPI(ctrl)

	// setup client and miner addrs
	minerAddr, err := address.NewIDAddress(1011)
	require.NoError(t, err)
	cAddr, err := address.NewIDAddress(1014)
	require.NoError(t, err)

	// instantiate the http servers that will serve the files
	normalServer := testutil.HttpTestUnstartedFileServer(t, dir)
	blockingServer := testutil.NewBlockingHttpTestServer(t, dir)
	disconnServer := testutil.HttpTestDisconnectingServer(t, dir, pc.disconnectAfterEvery)
	failingServer := testutil.HttpTestUnstartedFailingServer(t)

	// create a provider libp2p peer
	mn := mocknet.New()
	h, err := mn.GenPeer()
	require.NoError(t, err)

	// setup the databases
	f, err := ioutil.TempFile(dir, "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	sqldb, err := db.SqlDB(f.Name())
	require.NoError(t, err)
	dealsDB := db.NewDealsDB(sqldb)
	fundsDB := db.NewFundsDB(sqldb)

	// publish wallet
	pw, err := address.NewIDAddress(uint64(rand.Intn(100)))
	require.NoError(t, err)

	// create the harness with default values
	ph := &ProviderHarness{
		Host:                h,
		GoMockCtrl:          ctrl,
		TempDir:             dir,
		MinerAddr:           minerAddr,
		ClientAddr:          cAddr,
		NormalServer:        normalServer,
		BlockingServer:      blockingServer,
		DisconnectingServer: disconnServer,
		FailingServer:       failingServer,

		MockSealingPipelineAPI: sps,
		DealsDB:                dealsDB,
		FundsDB:                db.NewFundsDB(sqldb),
		StorageDB:              db.NewStorageDB(sqldb),
		PublishWallet:          pw,
		MinerStub:              minerStub,
		MinPublishFees:         pc.minPublishFees,
		MaxStagingDealBytes:    pc.maxStagingDealBytes,
		SqlDB:                  sqldb,
	}

	// fund manager
	fminitF := fundmanager.New(fundmanager.Config{
		PubMsgBalMin: ph.MinPublishFees,
		PubMsgWallet: pw,
	})
	fm := fminitF(fn, fundsDB)

	// storage manager
	fsRepo, err := repo.NewFS(dir)
	require.NoError(t, err)
	lr, err := fsRepo.Lock(repo.StorageMiner)
	require.NoError(t, err)
	smInitF := storagemanager.New(storagemanager.Config{
		MaxStagingDealsBytes: ph.MaxStagingDealBytes,
	})
	sm, err := smInitF(lr, sqldb)
	require.NoError(t, err)

	// no-op deal filter, as we are mostly testing the Provider and provider_loop here
	df := func(ctx context.Context, deal types.DealFilterParams) (bool, string, error) {
		return true, "", nil
	}

	ps, err := piecestoreimpl.NewPieceStore(dssync.MutexWrap(ds.NewMapDatastore()))
	require.NoError(t, err)
	dagStore := shared_testutil.NewMockDagStoreWrapper(ps, nil)

	askStore := &mockAskStore{}
	askStore.SetAsk(pc.price, pc.verifiedPrice, pc.minPieceSize, pc.maxPieceSize)

	prov, err := NewProvider(h, sqldb, dealsDB, fm, sm, fn, minerStub, minerAddr, minerStub, sps, minerStub, df, sqldb,
		db.NewLogsDB(sqldb), dagStore, ps, &NoOpIndexProvider{}, askStore, &mockSignatureVerifier{true, nil}, pc.httpOpts...)
	require.NoError(t, err)
	ph.Provider = prov

	fn.EXPECT().ChainHead(gomock.Any()).Return(&ctypes.TipSet{}, nil).AnyTimes()
	fn.EXPECT().StateDealProviderCollateralBounds(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(lapi.DealCollateralBounds{
		Min: abi.NewTokenAmount(1),
		Max: abi.NewTokenAmount(1),
	}, nil).AnyTimes()

	fn.EXPECT().StateMarketBalance(gomock.Any(), gomock.Any(), gomock.Any()).Return(lapi.MarketBalance{
		Locked: pc.lockedFunds,
		Escrow: pc.escrowFunds,
	}, nil).AnyTimes()

	fn.EXPECT().WalletBalance(gomock.Any(), ph.PublishWallet).Return(abi.NewTokenAmount(pc.publishWalletBal), nil).AnyTimes()

	ph.MockSealingPipelineAPI.EXPECT().WorkerJobs(gomock.Any()).Return(map[uuid.UUID][]storiface.WorkerJob{}, nil).AnyTimes()

	ph.MockSealingPipelineAPI.EXPECT().SectorsSummary(gomock.Any()).Return(sealingpipelineStatus, nil).AnyTimes()

	secInfo := lapi.SectorInfo{State: lapi.SectorState(sealing.Proving)}
	ph.MockSealingPipelineAPI.EXPECT().SectorsStatus(gomock.Any(), gomock.Any(), false).Return(secInfo, nil).AnyTimes()

	ph.DAGStore = dagStore
	ph.MockFullNode = fn

	return ph
}

func (h *ProviderHarness) shutdownAndCreateNewProvider(t *testing.T, ctx context.Context, opts ...harnessOpt) {
	pc := &providerConfig{
		minPublishFees:       abi.NewTokenAmount(100),
		maxStagingDealBytes:  10000000000,
		disconnectAfterEvery: 1048600,
		lockedFunds:          big.NewInt(300),
		escrowFunds:          big.NewInt(500),
		publishWalletBal:     1000,
	}
	for _, opt := range opts {
		opt(pc)
	}
	// shutdown old provider
	h.Provider.Stop()
	h.MinerStub = smtestutil.NewMinerStub(h.GoMockCtrl)
	// no-op deal filter, as we are mostly testing the Provider and provider_loop here
	df := func(ctx context.Context, deal types.DealFilterParams) (bool, string, error) {
		return true, "", nil
	}

	// construct a new provider with pre-existing state
	prov, err := NewProvider(h.Host, h.Provider.db, h.Provider.dealsDB, h.Provider.fundManager,
		h.Provider.storageManager, h.Provider.fullnodeApi, h.MinerStub, h.MinerAddr, h.MinerStub, h.MockSealingPipelineAPI, h.MinerStub,
		df, h.Provider.logsSqlDB, h.Provider.logsDB, h.Provider.dagst, h.Provider.ps, &NoOpIndexProvider{}, h.Provider.askGetter, h.Provider.sigVerifier, pc.httpOpts...)

	require.NoError(t, err)
	h.Provider = prov
}

func (h *ProviderHarness) Start(t *testing.T, ctx context.Context) {
	require.NoError(t, h.Provider.ps.Start(ctx))
	ready := make(chan error)
	h.Provider.ps.OnReady(func(err error) {
		ready <- err
	})

	require.NoError(t, <-ready)

	h.NormalServer.Start()
	h.BlockingServer.Start()
	h.DisconnectingServer.Start()
	h.FailingServer.Start()
	_, err := h.Provider.Start()
	require.NoError(t, err)
}

func (h *ProviderHarness) Stop() {
	_ = h.SqlDB.Close()
	h.FailingServer.Close()
	h.NormalServer.Close()
	h.BlockingServer.Close()
	h.DisconnectingServer.Close()
	h.GoMockCtrl.Finish()

}

type dealProposalConfig struct {
	normalFileSize     int
	offlineDeal        bool
	verifiedDeal       bool
	providerCollateral abi.TokenAmount
	minerAddr          address.Address
	pieceCid           cid.Cid
	pieceSize          abi.PaddedPieceSize
	undefinedPieceCid  bool
	startEpoch         abi.ChainEpoch
	endEpoch           abi.ChainEpoch
	label              string
}

// dealProposalOpt allows configuration of the deal proposal
type dealProposalOpt func(dc *dealProposalConfig)

// withNormalFileSize configures the deal proposal to use a normal file of the given size.
// note: the carv2 file size will be larger than this
func withNormalFileSize(normalFileSize int) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.normalFileSize = normalFileSize
	}
}

func withOfflineDeal() dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.offlineDeal = true
	}
}

func withVerifiedDeal() dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.verifiedDeal = true
	}
}

func withProviderCollateral(amt abi.TokenAmount) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.providerCollateral = amt
	}
}

func withMinerAddr(addr address.Address) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.minerAddr = addr
	}
}

func withPieceCid(c cid.Cid) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.pieceCid = c
	}
}

func withPieceSize(size abi.PaddedPieceSize) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.pieceSize = size
	}
}

func withUndefinedPieceCid() dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.undefinedPieceCid = true
	}
}

func withEpochs(start, end abi.ChainEpoch) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.startEpoch = start
		dc.endEpoch = end
	}
}

func withLabel(label string) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.label = label
	}
}

func (ph *ProviderHarness) newDealBuilder(t *testing.T, seed int, opts ...dealProposalOpt) *testDealBuilder {
	tbuilder := &testDealBuilder{t: t, ph: ph}

	dc := &dealProposalConfig{
		normalFileSize:     2000000,
		verifiedDeal:       false,
		providerCollateral: abi.NewTokenAmount(1),
		minerAddr:          tbuilder.ph.MinerAddr,
		pieceCid:           cid.Undef,
		undefinedPieceCid:  false,
		startEpoch:         abi.ChainEpoch(rand.Intn(100000)),
		endEpoch:           800000 + abi.ChainEpoch(rand.Intn(10000)),
	}
	for _, opt := range opts {
		opt(dc)
	}

	// generate a CARv2 file using a random seed in the tempDir
	randomFilepath, err := testutil.CreateRandomFile(tbuilder.ph.TempDir, seed, dc.normalFileSize)
	require.NoError(tbuilder.t, err)
	rootCid, carV2FilePath, err := testutil.CreateDenseCARv2(tbuilder.ph.TempDir, randomFilepath)
	require.NoError(tbuilder.t, err)

	// generate CommP of the CARv2 file
	cidAndSize, err := GenerateCommP(carV2FilePath)
	require.NoError(tbuilder.t, err)

	var pieceCid = cidAndSize.PieceCID
	if dc.pieceCid != cid.Undef {
		pieceCid = dc.pieceCid
	}
	if dc.undefinedPieceCid {
		pieceCid = cid.Undef
	}
	var pieceSize = cidAndSize.PieceSize
	if dc.pieceSize != abi.PaddedPieceSize(0) {
		pieceSize = dc.pieceSize
	}

	// build the deal proposal
	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize,
		VerifiedDeal:         dc.verifiedDeal,
		Client:               tbuilder.ph.ClientAddr,
		Provider:             dc.minerAddr,
		Label:                dc.label,
		StartEpoch:           dc.startEpoch,
		EndEpoch:             dc.endEpoch,
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   dc.providerCollateral,
		ClientCollateral:     abi.NewTokenAmount(1),
	}

	carv2Fileinfo, err := os.Stat(carV2FilePath)
	require.NoError(tbuilder.t, err)
	name := carv2Fileinfo.Name()

	// assemble the final deal params to send to the provider
	dealParams := &types.DealParams{
		DealUUID:  uuid.New(),
		IsOffline: dc.offlineDeal,
		ClientDealProposal: market.ClientDealProposal{
			Proposal: proposal,
			ClientSignature: acrypto.Signature{
				Type: acrypto.SigTypeBLS,
				Data: []byte("sig"),
			}, // We don't do signature verification in Boost SM testing.
		},
		DealDataRoot: rootCid,
		Transfer: types.Transfer{
			Type: "http",
			Size: uint64(carv2Fileinfo.Size()),
		},
	}

	td := &testDeal{
		ph:            tbuilder.ph,
		params:        dealParams,
		carv2FilePath: carV2FilePath,
		carv2FileName: name,
	}

	publishCid := testutil.GenerateCid()
	finalPublishCid := testutil.GenerateCid()
	dealId := abi.DealID(rand.Intn(100))
	sectorId := abi.SectorNumber(rand.Intn(100))
	offset := abi.PaddedPieceSize(rand.Intn(100))

	tbuilder.ms = tbuilder.ph.MinerStub.ForDeal(dealParams, publishCid, finalPublishCid, dealId, sectorId, offset)
	tbuilder.td = td
	return tbuilder
}

type minerStubCall struct {
	err      error
	blocking bool
}

type testDealBuilder struct {
	t  *testing.T
	td *testDeal
	ph *ProviderHarness

	ms               *smtestutil.MinerStubBuilder
	msNoOp           bool
	msPublish        *minerStubCall
	msPublishConfirm *minerStubCall
	msAddPiece       *minerStubCall
}

func (tbuilder *testDealBuilder) withPublishFailing(err error) *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{err: err}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishConfirmFailing(err error) *testDealBuilder {
	tbuilder.msPublishConfirm = &minerStubCall{err: err}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAddPieceFailing(err error) *testDealBuilder {
	tbuilder.msAddPiece = &minerStubCall{err: err}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishBlocking() *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{blocking: true}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishNonBlocking() *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishConfirmBlocking() *testDealBuilder {
	tbuilder.msPublishConfirm = &minerStubCall{blocking: true}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishConfirmNonBlocking() *testDealBuilder {
	tbuilder.msPublishConfirm = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAddPieceBlocking() *testDealBuilder {
	tbuilder.msAddPiece = &minerStubCall{blocking: true}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAddPieceNonBlocking() *testDealBuilder {
	tbuilder.msAddPiece = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAllMinerCallsNonBlocking() *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{blocking: false}
	tbuilder.msPublishConfirm = &minerStubCall{blocking: false}
	tbuilder.msAddPiece = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAllMinerCallsBlocking() *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{blocking: true}
	tbuilder.msPublishConfirm = &minerStubCall{blocking: true}
	tbuilder.msAddPiece = &minerStubCall{blocking: true}

	return tbuilder
}

func (tbuilder *testDealBuilder) withFailingHttpServer() *testDealBuilder {
	tbuilder.setTransferParams(tbuilder.td.ph.FailingServer.URL)
	return tbuilder
}

func (tbuilder *testDealBuilder) withBlockingHttpServer() *testDealBuilder {
	tbuilder.ph.BlockingServer.AddFile(tbuilder.td.carv2FileName)
	tbuilder.setTransferParams(tbuilder.td.ph.BlockingServer.URL)
	return tbuilder
}

func (tbuilder *testDealBuilder) withNoOpMinerStub() *testDealBuilder {
	tbuilder.msNoOp = true
	return tbuilder
}

func (tbuilder *testDealBuilder) withDisconnectingHttpServer() *testDealBuilder {
	tbuilder.setTransferParams(tbuilder.ph.DisconnectingServer.URL)
	return tbuilder
}

func (tbuilder *testDealBuilder) withNormalHttpServer() *testDealBuilder {
	tbuilder.setTransferParams(tbuilder.ph.NormalServer.URL)
	return tbuilder
}

func (tbuilder *testDealBuilder) setTransferParams(serverURL string) {
	transferParams := &types2.HttpRequest{URL: serverURL + "/" + filepath.Base(tbuilder.td.carv2FilePath)}
	transferParamsJSON, err := json.Marshal(transferParams)
	if err != nil {
		panic(err)
	}
	tbuilder.td.params.Transfer.Params = transferParamsJSON
}

func (tbuilder *testDealBuilder) build() *testDeal {
	// if the miner stub is supposed to be a no-op, setup a no-op and don't build any other stub behaviour
	if tbuilder.msNoOp {
		tbuilder.ms.SetupNoOp()
	} else {
		tbuilder.buildPublish().buildPublishConfirm().buildAddPiece()
	}

	testDeal := tbuilder.td

	testDeal.stubOutput = tbuilder.ms.Output()
	testDeal.tBuilder = tbuilder
	return testDeal
}

func (tbuilder *testDealBuilder) buildPublish() *testDealBuilder {
	if tbuilder.msPublish != nil {
		if err := tbuilder.msPublish.err; err != nil {
			tbuilder.ms.SetupPublishFailure(err)
		} else {
			tbuilder.ms.SetupPublish(tbuilder.msPublish.blocking)
		}
	}
	return tbuilder
}

func (tbuilder *testDealBuilder) buildPublishConfirm() *testDealBuilder {
	if tbuilder.msPublishConfirm != nil {
		if err := tbuilder.msPublishConfirm.err; err != nil {
			tbuilder.ms.SetupPublishConfirmFailure(err)
		} else {
			tbuilder.ms.SetupPublishConfirm(tbuilder.msPublishConfirm.blocking)
		}
	}

	return tbuilder
}

func (tbuilder *testDealBuilder) buildAddPiece() *testDealBuilder {
	if tbuilder.msAddPiece != nil {
		if err := tbuilder.msAddPiece.err; err != nil {
			tbuilder.ms.SetupAddPieceFailure(err)
		} else {
			tbuilder.ms.SetupAddPiece(tbuilder.msAddPiece.blocking)
		}
	}

	return tbuilder
}

type testDeal struct {
	ph            *ProviderHarness
	params        *types.DealParams
	carv2FilePath string
	carv2FileName string
	stubOutput    *smtestutil.StubbedMinerOutput
	sub           event.Subscription

	tBuilder *testDealBuilder
}

func (td *testDeal) executeAndSubscribeImportOfflineDeal() error {
	pi, dh, err := td.ph.Provider.ImportOfflineDealData(td.params.DealUUID, td.carv2FilePath)
	if err != nil {
		return err
	}
	if !pi.Accepted {
		return errors.New("deal not accepted")
	}
	sub, err := dh.subscribeUpdates()
	if err != nil {
		return err
	}
	td.sub = sub

	return nil
}

func (td *testDeal) executeAndSubscribe() error {
	pi, dh, err := td.ph.Provider.ExecuteDeal(td.params, peer.ID(""))
	if err != nil {
		return err
	}
	if !pi.Accepted {
		return fmt.Errorf("deal not accepted: %s", pi.Reason)
	}
	sub, err := dh.subscribeUpdates()
	if err != nil {
		return err
	}
	td.sub = sub

	return nil
}

func (td *testDeal) waitForError(errContains string) error {
	if td.sub == nil {
		return errors.New("no subcription for deal")
	}

	for i := range td.sub.Out() {
		st := i.(types.ProviderDealState)
		if len(st.Err) != 0 {
			if !strings.Contains(st.Err, errContains) {
				return fmt.Errorf("actual error does not contain expected error, expected: %s, actual:%s", errContains, st.Err)
			}

			return nil
		}
	}

	return errors.New("did not get any error")
}

func (td *testDeal) waitForCheckpoint(cp dealcheckpoints.Checkpoint) error {
	if td.sub == nil {
		return errors.New("no subcription for deal")
	}

LOOP:
	for i := range td.sub.Out() {
		st := i.(types.ProviderDealState)
		if len(st.Err) != 0 {
			return errors.New(st.Err)
		}
		if st.Checkpoint == cp {
			break LOOP
		}
	}

	return nil
}

func (td *testDeal) waitForSealingState(secState lapi.SectorState) error {
	if td.sub == nil {
		return errors.New("no subcription for deal")
	}

	for i := range td.sub.Out() {
		st := i.(types.ProviderDealState)
		if len(st.Err) != 0 {
			return errors.New(st.Err)
		}
		si, err := td.ph.MockSealingPipelineAPI.SectorsStatus(context.Background(), st.SectorID, false)
		if err != nil {
			return err
		}
		if si.State == secState {
			return nil
		}
	}

	return fmt.Errorf("did not reach sealing state %s", secState)
}

func (td *testDeal) updateWithRestartedProvider(ph *ProviderHarness) *testDealBuilder {
	old := td.stubOutput

	td.ph = ph
	td.tBuilder.msPublish = nil
	td.tBuilder.msAddPiece = nil
	td.tBuilder.msPublishConfirm = nil

	td.tBuilder.ph = ph
	td.tBuilder.td = td
	td.tBuilder.ms = ph.MinerStub.ForDeal(td.params, old.PublishCid, old.FinalPublishCid, old.DealID, old.SectorID, old.Offset)

	return td.tBuilder
}

func (td *testDeal) waitForAndAssert(t *testing.T, ctx context.Context, cp dealcheckpoints.Checkpoint) {
	require.NoError(t, td.waitForCheckpoint(cp))

	switch cp {
	case dealcheckpoints.Accepted:
		td.ph.AssertAccepted(t, ctx, td.params)
	case dealcheckpoints.Transferred:
		td.ph.AssertTransferred(t, ctx, td.params)
	case dealcheckpoints.Published:
		td.ph.AssertPublished(t, ctx, td.params, td.stubOutput)
	case dealcheckpoints.PublishConfirmed:
		td.ph.AssertPublishConfirmed(t, ctx, td.params, td.stubOutput)
	case dealcheckpoints.AddedPiece:
		td.ph.AssertPieceAdded(t, ctx, td.params, td.stubOutput, td.carv2FilePath)
	default:
		t.Fail()
	}
}

func (td *testDeal) unblockTransfer() {
	td.ph.BlockingServer.UnblockFile(td.carv2FileName)
}

func (td *testDeal) unblockPublish() {
	td.ph.MinerStub.UnblockPublish(td.params.DealUUID)
}

func (td *testDeal) unblockWaitForPublish() {
	td.ph.MinerStub.UnblockWaitForPublish(td.params.DealUUID)
}

func (td *testDeal) unblockAddPiece() {
	td.ph.MinerStub.UnblockAddPiece(td.params.DealUUID)
}

func (td *testDeal) assertPieceAdded(t *testing.T, ctx context.Context) {
	td.ph.AssertPieceAdded(t, ctx, td.params, td.stubOutput, td.carv2FilePath)
}

func (td *testDeal) assertDealPublished(t *testing.T, ctx context.Context) {
	td.ph.AssertPublished(t, ctx, td.params, td.stubOutput)
}

func (td *testDeal) assertDealFailedTransferNonRecoverable(t *testing.T, ctx context.Context, errStr string) {
	td.ph.AssertDealFailedTransferNonRecoverable(t, ctx, td.params, errStr)
}

func (td *testDeal) assertEventuallyDealCleanedup(t *testing.T, ctx context.Context) {
	td.ph.AssertEventuallyDealCleanedup(t, ctx, td.params)
}

func (td *testDeal) assertDealFailedNonRecoverable(t *testing.T, ctx context.Context, errContains string) {
	dbState, err := td.ph.DealsDB.ByID(ctx, td.params.DealUUID)
	require.NoError(t, err)

	require.NotEmpty(t, dbState.Err)
	require.Contains(t, dbState.Err, errContains)
	require.EqualValues(t, dealcheckpoints.Complete, dbState.Checkpoint)
}

type NoOpIndexProvider struct{}

func (n *NoOpIndexProvider) AnnounceBoostDeal(ctx context.Context, pds *types.ProviderDealState) (cid.Cid, error) {
	return testutil.GenerateCid(), nil
}

func (n *NoOpIndexProvider) Start(_ context.Context) {

}

type mockAskStore struct {
	ask *storagemarket.StorageAsk
}

func (m *mockAskStore) SetAsk(price, verifiedPrice abi.TokenAmount, minPieceSize, maxPieceSize abi.PaddedPieceSize) {
	m.ask = &storagemarket.StorageAsk{
		Price:         price,
		VerifiedPrice: verifiedPrice,
		MinPieceSize:  minPieceSize,
		MaxPieceSize:  maxPieceSize,
	}

}

func (m *mockAskStore) GetAsk() *storagemarket.SignedStorageAsk {
	return &storagemarket.SignedStorageAsk{
		Ask: m.ask,
	}
}

type mockSignatureVerifier struct {
	valid bool
	err   error
}

func (m *mockSignatureVerifier) VerifySignature(ctx context.Context, sig acrypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error) {
	return m.valid, m.err
}

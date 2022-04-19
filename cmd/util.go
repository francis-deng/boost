package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/urfave/cli/v2"
	"regexp"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

func resolveMinerMultiaddr(cctx *cli.Context) (*peer.AddrInfo, bool){
	if !cctx.IsSet("miner-p2p-address") {
		return nil,false
	}

	mP2pAddrStr := cctx.String("miner-p2p-address")
	re := regexp.MustCompile("(.+)/p2p/(.+)")

	idAndMa := re.FindStringSubmatch(mP2pAddrStr)
	id,err := peer.IDFromString(idAndMa[1])
	if err != nil {
		return nil,false
	}

	ma,err := multiaddr.NewMultiaddr(idAndMa[2])
	if err != nil {
		return nil,false
	}

	var maddrs []multiaddr.Multiaddr
	maddrs = append(maddrs, ma)

	return &peer.AddrInfo{
		ID:    id,
		Addrs: maddrs,
	}, true
}

func GetMinerAddrInfo(cctx *cli.Context, ctx context.Context, api api.Gateway) (*peer.AddrInfo, error) {
	pa,worked := resolveMinerMultiaddr(cctx)
	if worked {
		return pa, nil
	}

	maddr, err := address.NewFromString(cctx.String("provider"))
	if err != nil {
		return nil, err
	}

	return GetAddrInfo(ctx,api,maddr)
}

func GetAddrInfo(ctx context.Context, api api.Gateway, maddr address.Address) (*peer.AddrInfo, error) {
	minfo, err := api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}
	if minfo.PeerId == nil {
		return nil, fmt.Errorf("storage provider %s has no peer ID set on-chain", maddr)
	}

	var maddrs []multiaddr.Multiaddr
	for _, mma := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(mma)
		if err != nil {
			return nil, fmt.Errorf("storage provider %s had invalid multiaddrs in their info: %w", maddr, err)
		}
		maddrs = append(maddrs, ma)
	}
	if len(maddrs) == 0 {
		return nil, fmt.Errorf("storage provider %s has no multiaddrs set on-chain", maddr)
	}

	return &peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}, nil
}

func PrintJson(obj interface{}) error {
	resJson, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling json: %w", err)
	}

	fmt.Println(string(resJson))
	return nil
}

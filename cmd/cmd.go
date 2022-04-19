package cmd

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var FlagRepo = &cli.StringFlag{
	Name:  "repo",
	Usage: "repo directory for Boost client",
	Value: "~/.boost-client",
}

var FlagJson = &cli.BoolFlag{
	Name:  "json",
	Usage: "output results in json format",
	Value: false,
}


var log = logging.Logger("cmd")
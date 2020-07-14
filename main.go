package main

import (
	"cache-updater/cacher"
	"cache-updater/config"
	"context"
	"flag"

	"github.com/go-pg/pg/v10"
)

const (
	DefaultConfigFileName = "config.json"
)

func main() {
	var confFileName string
	var startHeight uint64
	flag.StringVar(&confFileName, "config", DefaultConfigFileName, "set config path")
	flag.Uint64Var(&startHeight, "height", 0, "set start scan height")
	flag.Parse()

	cfg, err := config.Load(confFileName)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	db := pg.Connect(&pg.Options{
		Addr:     cfg.DB.Addr,
		User:     cfg.DB.User,
		Password: cfg.DB.Password,
		Database: cfg.DB.Database,
	})
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		panic(err)
	}

	var startHeightOpt *cacher.StartHeightOpt
	if startHeight != 0 {
		startHeightOpt = &cacher.StartHeightOpt{
			Height: startHeight,
		}
	}

	ethereum, err := cacher.NewEthereumCacher(ctx, cfg.Chains[string(cacher.Ethereum)].Host, cfg.Nebulae[cacher.Ethereum])
	if err != nil {
		panic(err)
	}
	go cacher.Start(ethereum, db, cfg.Chains[string(cacher.Ethereum)].IntervalHeight, startHeightOpt)

	waves, err := cacher.NewWavesCacher(cfg.Chains[string(cacher.Waves)].Host, cfg.Nebulae[cacher.Waves])
	if err != nil {
		panic(err)
	}
	go cacher.Start(waves, db, cfg.Chains[string(cacher.Ethereum)].IntervalHeight, startHeightOpt)

	ledger, err := cacher.NewLedgerCache(cfg.Chains[string(cacher.Ledger)].Host, cfg.Nebulae)
	if err != nil {
		panic(err)
	}
	go cacher.Start(ledger, db, 0, startHeightOpt)
}

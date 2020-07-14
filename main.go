package main

import (
	"cache-updater/cacher"
	"cache-updater/config"
	"context"
	"crypto/tls"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-pg/pg/v10"
)

const (
	DefaultConfigFileName = "config.json"
)

func main() {
	var confFileName string
	var startEthHeight, startWavesHeight, startLedgerHeight uint64
	flag.StringVar(&confFileName, "config", DefaultConfigFileName, "set config path")
	flag.Uint64Var(&startEthHeight, "ethHeight", 0, "set start scan height")
	flag.Uint64Var(&startWavesHeight, "wavesHeight", 0, "set start scan height")
	flag.Uint64Var(&startLedgerHeight, "ledgerHeight", 0, "set start scan height")
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
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		panic(err)
	}

	var startEthHeightOpt *cacher.StartHeightOpt
	if startEthHeight != 0 {
		startEthHeightOpt = &cacher.StartHeightOpt{
			Height: startEthHeight,
		}
	}
	ethereum, err := cacher.NewEthereumCacher(ctx, cfg.Chains[string(cacher.Ethereum)].Host, cfg.Nebulae[cacher.Ethereum])
	if err != nil {
		panic(err)
	}
	go cacher.Start(ethereum, db, cfg.Chains[string(cacher.Ethereum)].IntervalHeight, startEthHeightOpt)

	var startWavesHeightOpt *cacher.StartHeightOpt
	if startWavesHeight != 0 {
		startWavesHeightOpt = &cacher.StartHeightOpt{
			Height: startWavesHeight,
		}
	}
	waves, err := cacher.NewWavesCacher(cfg.Chains[string(cacher.Waves)].Host, cfg.Nebulae[cacher.Waves], ctx)
	if err != nil {
		panic(err)
	}
	go cacher.Start(waves, db, cfg.Chains[string(cacher.Ethereum)].IntervalHeight, startWavesHeightOpt)

	var startLedgerHeightOpt *cacher.StartHeightOpt
	if startLedgerHeight != 0 {
		startLedgerHeightOpt = &cacher.StartHeightOpt{
			Height: startLedgerHeight,
		}
	}
	ledger, err := cacher.NewLedgerCache(cfg.Chains[string(cacher.Ledger)].Host, cfg.Nebulae)
	if err != nil {
		panic(err)
	}
	go cacher.Start(ledger, db, 0, startLedgerHeightOpt)

	for {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		os.Exit(0)
	}
}

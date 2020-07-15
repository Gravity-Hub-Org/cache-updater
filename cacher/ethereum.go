package cacher

import (
	"cache-updater/contracts"
	"cache-updater/keys"
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum/go-ethereum/ethclient"
)

type EthereumCacher struct {
	client  *ethclient.Client
	ctx     context.Context
	nebulae []string
}

func NewEthereumCacher(ctx context.Context, host string, nebulae []string) (*EthereumCacher, error) {
	client, err := ethclient.DialContext(ctx, host)
	if err != nil {
		return nil, err
	}

	return &EthereumCacher{
		client:  client,
		ctx:     ctx,
		nebulae: nebulae,
	}, nil
}

func (cacher *EthereumCacher) GetType() CacherType {
	return Ethereum
}

func (cacher *EthereumCacher) GetLastHeight() (uint64, error) {
	block, err := cacher.client.BlockByNumber(cacher.ctx, nil)
	if err != nil {
		return 0, err
	}

	return uint64(block.Number().Int64()), nil
}

func (cacher *EthereumCacher) GetBlockHash(height uint64) (string, error) {
	block, err := cacher.client.BlockByNumber(cacher.ctx, big.NewInt(int64(height)))
	if err != nil {
		return "", err
	}

	return block.Hash().Hex(), nil
}

func (cacher *EthereumCacher) GetData(height uint64) (map[string]Data, error) {
	data := make(map[string]Data)
	for _, nebula := range cacher.nebulae {
		contract, err := contracts.NewNebula(common.HexToAddress(nebula), cacher.client)
		if err != nil {
			return nil, err
		}

		iterator, err := contract.FilterNewPulse(&bind.FilterOpts{
			Start:   height,
			End:     &height,
			Context: cacher.ctx,
		})
		if err != nil {
			return nil, err
		}

		for iterator.Next() {
			data[keys.FormPulse(nebula, fmt.Sprintf("%s", iterator.Event.Height))] = Data{
				Type:  StringType,
				Value: hexutil.Encode(iterator.Event.DataHash[:]),
			}
		}
	}

	return data, nil
}

package cacher

import (
	"cache-updater/keys"
	"context"
	"fmt"

	"github.com/Gravity-Hub-Org/proof-of-concept/gh-node/helpers"
	"github.com/wavesplatform/gowaves/pkg/client"
)

type WavesCacher struct {
	helper  *helpers.Node
	nebulae []string
	client  *client.Client
	ctx     context.Context
}

func NewWavesCacher(host string, nebulae []string, ctx context.Context) (*WavesCacher, error) {
	helper := helpers.New(host, "")
	wavesClient, err := client.NewClient(client.Options{ApiKey: "", BaseUrl: host})
	if err != nil {
		return nil, err
	}

	return &WavesCacher{client: wavesClient, helper: &helper, nebulae: nebulae, ctx: ctx}, nil
}

func (cacher *WavesCacher) GetType() CacherType {
	return Waves
}

func (cacher *WavesCacher) GetLastHeight() (uint64, error) {
	b, _, err := cacher.client.Blocks.Height(cacher.ctx)
	if err != nil {
		return 0, err
	}

	return b.Height, nil
}

func (cacher *WavesCacher) GetBlockHash(height uint64) (string, error) {
	b, _, err := cacher.client.Blocks.At(cacher.ctx, height)
	if err != nil {
		return "", err
	}

	return b.Signature.String(), nil
}

func (cacher *WavesCacher) GetData(height uint64) (map[string]Data, error) {
	data := make(map[string]Data)
	for _, nebula := range cacher.nebulae {
		heightString := fmt.Sprintf("%d", height)
		pulse, err := cacher.helper.GetStateByAddressAndKey(nebula, heightString)
		if err != nil {
			return nil, err
		}

		if pulse == nil {
			return data, nil
		}
		data[keys.FormPulse(nebula, heightString)] = Data{
			Type:  StringType,
			Value: pulse.Value.(string),
		}
	}
	return data, nil
}

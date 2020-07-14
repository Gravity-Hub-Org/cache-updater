package cacher

import (
	"encoding/binary"
	"encoding/json"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"

	cacheKeys "cache-updater/keys"

	"github.com/Gravity-Hub-Org/proof-of-concept/common/keys"
	"github.com/mr-tron/base58"

	rpchttp "github.com/tendermint/tendermint/rpc/client/http"
)

const (
	keyPath   string = "key"
	prefixKey string = "prefix"
)

type LedgerCacher struct {
	client  *rpchttp.HTTP
	nebulae map[CacherType][]string
}

func NewLedgerCache(host string, nebulae map[CacherType][]string) (*LedgerCacher, error) {
	client, err := rpchttp.New(host, "/websocket")
	if err != nil {
		return nil, err
	}
	return &LedgerCacher{client: client, nebulae: nebulae}, nil
}

func (cacher *LedgerCacher) GetType() CacherType {
	return Ledger
}

func (cacher *LedgerCacher) GetLastHeight() (uint64, error) {
	status, err := cacher.client.Status()
	if err != nil {
		return 0, err
	}
	return uint64(status.SyncInfo.LatestBlockHeight), nil
}

func (cacher *LedgerCacher) GetBlockHash(height uint64) (string, error) {
	intHeight := int64(height)
	rs, err := cacher.client.Block(&intHeight)
	if err != nil {
		return "", err
	}
	return rs.BlockID.String(), nil
}

func (cacher *LedgerCacher) GetData(height uint64) (map[string]Data, error) {
	data := make(map[string]Data)

	rs, err := cacher.client.ABCIQuery(keyPath, []byte(keys.FormConsulsKey()))
	if err != nil {
		return nil, err
	}

	var consuls []interface{}
	if rs.Response.Value != nil {
		err = json.Unmarshal(rs.Response.Value, &consuls)
		if err != nil {
			return nil, err
		}
	}

	b, err := json.Marshal(consuls)
	if err != nil {
		return nil, err
	}

	data[keys.FormConsulsKey()] = Data{
		Type:  JsonType,
		Value: b,
	}

	for t, v := range cacher.nebulae {
		for _, nebula := range v {
			var nebulaId []byte
			var err error
			switch t {
			case Waves:
				nebulaId, err = base58.Decode(nebula)
				if err != nil {
					return nil, err
				}
			case Ethereum:
				nebulaId, err = hexutil.Decode(nebula)
				if err != nil {
					return nil, err
				}
			}

			//Oracles by nebula
			rs, err := cacher.client.ABCIQuery(keyPath, []byte(keys.FormOraclesByNebulaKey(nebulaId)))
			if err != nil {
				return nil, err
			}

			var oracles map[string]string
			if rs.Response.Value != nil {
				err = json.Unmarshal(rs.Response.Value, &oracles)
				if err != nil {
					return nil, err
				}
			}

			b, err := json.Marshal(oracles)
			if err != nil {
				return nil, err
			}

			data[cacheKeys.FormOraclesByNebula(nebula)] = Data{
				Type:  JsonType,
				Value: b,
			}

			//Scores
			rs, err = cacher.client.ABCIQuery(prefixKey, []byte(keys.ScoreKey))
			if err != nil {
				return nil, err
			}

			var scores map[string][]byte
			if rs.Response.Value != nil {
				err = json.Unmarshal(rs.Response.Value, &scores)
				if err != nil {
					return nil, err
				}
			}

			for k, v := range scores {
				validator := strings.Split(k, "_")[1]
				data[cacheKeys.FormScores(validator)] = Data{
					Type:  IntType,
					Value: binary.BigEndian.Uint64(v),
				}
			}
		}
	}

	return data, nil
}

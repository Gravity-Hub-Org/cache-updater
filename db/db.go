package db

import (
	"cache-updater/cacher"
	"cache-updater/keys"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Gravity-Hub-Org/gravity-node-api-mockup/v2/model"

	"github.com/go-pg/pg/v10"
)

type DBHelper struct {
	Db         *pg.DB
	NebulaeMap map[cacher.CacherType][]string
}

func (helper *DBHelper) Nebulae() ([]model.Nebula, error) {
	var nebulae []model.Nebula
	for k, v := range helper.NebulaeMap {
		for i, nebula := range v {
			nodes, err := helper.validatorsByNebula(nebula)
			if err != nil {
				return nil, err
			}

			var score int
			for _, v := range nodes {
				vScore, err := helper.Score(v)
				if err != nil {
					return nil, err
				}
				score += int(vScore)
			}
			score /= len(nodes)

			var chainType model.ChainType
			switch k {
			case cacher.Waves:
				chainType = model.WAVES_TARGET_CHAIN
			case cacher.Ethereum:
				chainType = model.ETH_TARGET_CHAIN
			}

			nebulae = append(nebulae, model.Nebula{
				Address:         nebula,
				Status:          model.NebulaActiveStatus,
				Name:            fmt.Sprintf("Test Nebula #%d", i),
				Score:           model.Score(score),
				Description:     "Test nebula",
				SubscriptionFee: "10",
				NodesUsing:      nodes,
				Regularity:      0,
				TargetChain:     chainType,
			})
		}
	}

	return nebulae, nil
}

func (helper *DBHelper) Nodes() ([]model.Node, error) {
	var nodes []model.Node
	nodeAddresses, err := helper.nodeAddresses()
	if err != nil {
		return nil, err
	}

	for i, v := range nodeAddresses {
		vScore, err := helper.Score(v)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, model.Node{
			Address:       v,
			Name:          fmt.Sprintf("Test Node #%d", i),
			Score:         model.Score(vScore),
			Description:   "Test nebula",
			DepositAmount: 1000,
			DepositChain:  model.WAVES_TARGET_CHAIN,
			JoinedAt:      time.Now().Unix(),
			LockedUntil:   time.Now().Unix(),
			NebulasUsing:  []string{},
		})
	}

	return nodes, nil
}

func (helper *DBHelper) CommonStatus() (model.CommonStats, error) {
	var status model.CommonStats
	consulsData := new([]cacher.DataLog)
	v, err := helper.Db.Model(consulsData).Where("key like ?", keys.PulseKey+"%").SelectAndCount()
	if err != nil {
		return model.CommonStats{}, err
	}

	status.Pulses = uint(v)
	status.DataFeeds = 2

	nodes, err := helper.nodeAddresses()
	if err != nil {
		return status, err
	}

	status.NodesCount = uint(len(nodes))
	return status, nil
}

func (helper *DBHelper) nodeAddresses() ([]string, error) {
	allNodes := make(map[string]bool)

	consulsData := new(cacher.DataLog)
	err := helper.Db.Model(consulsData).Where("key = ?", keys.FormConsuls()).Order("height DESC").Limit(1).Select()
	if err != nil {
		return nil, err
	}

	if consulsData.Key != "" {
		var consuls []interface{}

		err = json.Unmarshal([]byte(consulsData.Value), &consuls)
		if err != nil {
			return nil, err
		}

		for _, v := range consuls {
			j := v.(map[string]interface{})
			allNodes[j["Validator"].(string)] = true
		}
	}

	for _, v := range helper.NebulaeMap {
		for _, nebula := range v {
			nodes, err := helper.validatorsByNebula(nebula)
			if err != nil {
				return nil, err
			}

			for _, v := range nodes {
				if _, ok := allNodes[v]; ok {
					continue
				}

				allNodes[v] = true
			}
		}
	}
	var result []string

	for k, _ := range allNodes {
		result = append(result, k)
	}
	return result, nil
}

func (helper *DBHelper) validatorsByNebula(nebula string) ([]string, error) {
	var result []string
	consulsData := new(cacher.DataLog)
	err := helper.Db.Model(consulsData).Where("key = ?", keys.FormOraclesByNebula(nebula)).Order("height DESC").Limit(1).Select()
	if err != nil {
		return nil, err
	}

	var oracles map[string]string
	if consulsData.Key != "" {
		b := []byte(consulsData.Value)

		err = json.Unmarshal(b, &oracles)
		if err != nil {
			return nil, err
		}

		for _, v := range oracles {
			result = append(result, v)
		}
	}

	return result, nil
}

func (helper *DBHelper) Score(validator string) (uint64, error) {
	consulsData := new(cacher.DataLog)
	err := helper.Db.Model(consulsData).Where("key = ?", keys.FormScores(validator)).Order("height DESC").Limit(1).Select()
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(consulsData.Value, 10, 32)
	if err != nil {
		return 0, err
	}

	return v, err
}

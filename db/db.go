package db

import (
	"cache-updater/cacher"
	"cache-updater/keys"
	"encoding/json"
	"fmt"
	"github.com/Gravity-Hub-Org/gravity-node-api-mockup/v2/model"
	"strconv"

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
				return nebulae, err
			}

			if len(nodes) <= 0 {
				return nebulae, err
			}
			var score int
			for _, v := range nodes {
				vScore, err := helper.Score(v)
				if err != nil {
					return nebulae, err
				}
				score += int(vScore)
			}
			score /= len(nodes)

			fee := ""
			var chainType model.ChainType
			switch k {
			case cacher.Waves:
				chainType = model.WAVES_TARGET_CHAIN
				fee = "100000000"
			case cacher.Ethereum:
				chainType = model.ETH_TARGET_CHAIN
				fee = "700000"
			}


			nebulae = append(nebulae, model.Nebula{
				Address:         nebula,
				Status:          model.NebulaActiveStatus,
				Name:            fmt.Sprintf("Demo Nebula #%d", i + chainType + 1),
				Score:           model.Score(score),
				Description:     "A demo network precofigured smart contract that provides BTC/USD price data.",
				SubscriptionFee: fee,
				NodesUsing:      nodes,
				Regularity:      1,
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

	for i, address := range nodeAddresses {
		vScore, err := helper.Score(address)
		if err != nil {
			return nil, err
		}

		var nebulae []string
		for _, v := range helper.NebulaeMap {
			for _, nebula := range v {
				nodes, err := helper.validatorsByNebula(nebula)
				if err != nil {
					return nil, err
				}

				for _, node := range nodes {
					if address == node {
						nebulae = append(nebulae, nebula)
					}
				}
			}
		}


		switch i {
		case 0:
			nodes = append(nodes, model.Node{
				Address:       address,
				Name:          "#1 - Neutrino Demo Node",
				Score:         model.Score(vScore),
				Description:   "Test node provided by Neutrino Protocol.",
				DepositAmount: 1000,
				DepositChain:  model.ETH_TARGET_CHAIN,
				JoinedAt:      1595192400,
				LockedUntil:   0,
				NebulasUsing:  nebulae,
			})
		case 1:
			nodes = append(nodes, model.Node{
				Address:       address,
				Name:          "#2 - Band Demo Node",
				Score:         model.Score(vScore),
				Description:   "Test node provided by Band Protocol",
				DepositAmount: 7,
				DepositChain:  model.ETH_TARGET_CHAIN,
				JoinedAt:      1595192400,
				LockedUntil:   0,
				NebulasUsing:  nebulae,
			})
		case 2:
			nodes = append(nodes, model.Node{
				Address:       address,
				Name:          "#3 - VenLab Demo Node",
				Score:         model.Score(vScore),
				Description:   "Test node provided by Ventuary Lab",
				DepositAmount: 7,
				DepositChain:  model.ETH_TARGET_CHAIN,
				JoinedAt:      1595192400,
				LockedUntil:   1595192400,
				NebulasUsing:  nebulae,
			})
		case 3:
			nodes = append(nodes, model.Node{
				Address:       address,
				Name:          "#4 - Gravity Demo Node",
				Score:         model.Score(vScore),
				Description:   "Test node provided by Gravity Protocol",
				DepositAmount: 1000,
				DepositChain:  model.WAVES_TARGET_CHAIN,
				JoinedAt:      1595192400,
				LockedUntil:   1595192400,
				NebulasUsing:  nebulae,
			})
		case 4:
			nodes = append(nodes, model.Node{
				Address:       address,
				Name:          "#5 - WX Demo Node",
				Score:         model.Score(vScore),
				Description:   "Test node provided by Waves.Exchange",
				DepositAmount: 1000,
				DepositChain:  model.WAVES_TARGET_CHAIN,
				JoinedAt:      1595192400,
				LockedUntil:   1595192400,
				NebulasUsing:  nebulae,
			})
		}
	}

	return nodes, nil
}

func (helper *DBHelper) CommonStatus() (pulses uint, nodeCount uint, err error) {
	consulsData := new([]cacher.DataLog)
	v, err := helper.Db.Model(consulsData).Where("key like ?", keys.PulseKey+"%").SelectAndCount()
	if err != nil {
		return 0,0, err
	}

	pulses = uint(v)

	nodes, err := helper.nodeAddresses()
	if err != nil {
		return 0,0, err
	}

	nodeCount = uint(len(nodes))
	return pulses,nodeCount, err
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
	helper.Db.Model(consulsData).Where("key = ?", keys.FormOraclesByNebula(nebula)).Order("height DESC").Limit(1).Select()

	var oracles map[string]string
	if consulsData.Key != "" {
		b := []byte(consulsData.Value)

		err := json.Unmarshal(b, &oracles)
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

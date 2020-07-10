package db

import (
	"cache-updater/cacher"
	"cache-updater/keys"
	"encoding/json"
	"strconv"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/go-pg/pg/v10"
)

type DBHelper struct {
	db *pg.DB
}

func (helper *DBHelper) Consuls() ([]string, error) {
	var result []string
	consulsData := new(cacher.DataLog)
	err := helper.db.Model(consulsData).Where("key", keys.FormConsuls()).Order("height DESC").Limit(1).Select()
	if err != nil {
		return nil, err
	}

	if consulsData.Key != "" {
		var consuls []interface{}
		b, err := hexutil.Decode(consulsData.Value)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(b, &consuls)
		if err != nil {
			return nil, err
		}

		for _, v := range consuls {
			j := v.(map[string]interface{})
			result = append(result, j["validator"].(string))
		}
	}

	return result, nil
}

func (helper *DBHelper) PulsesCount() (int, error) {
	consulsData := new(cacher.DataLog)
	v, err := helper.db.Model(consulsData).Where("key like ?", keys.PulseKey).Count()
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (helper *DBHelper) OraclesByNebula(nebula string) ([]string, error) {
	var result []string
	consulsData := new(cacher.DataLog)
	err := helper.db.Model(consulsData).Where("key", keys.FormOraclesByNebula(nebula)).Order("height DESC").Limit(1).Select()
	if err != nil {
		return nil, err
	}

	var oracles map[string]string
	if consulsData.Key != "" {
		b, err := hexutil.Decode(consulsData.Value)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(b, &oracles)
		if err != nil {
			return nil, err
		}

		for k, _ := range oracles {
			result = append(result, k)
		}
	}

	return result, nil
}

func (helper *DBHelper) Score(validator string) (uint64, error) {
	consulsData := new(cacher.DataLog)
	err := helper.db.Model(consulsData).Where("key", keys.FormScores(validator)).Order("height DESC").Limit(1).Select()
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(consulsData.Value, 10, 32)
	if err != nil {
		return 0, err
	}

	return v, err
}

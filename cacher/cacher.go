package cacher

import (
	"fmt"
	"time"

	"github.com/go-pg/pg/v10"
)

type ValueType string
type CacherType string

type Data struct {
	Type  ValueType
	Value interface{}
}

type DataLog struct {
	BlockHash string `pg:"block"`
	Key       string `pg:"key"`
	ChainType string `pg:"chain"`
	Type      string `pg:"type"`
	Value     string `pg:"value"`
	Height    int    `pg:"height"`
}

const (
	IntType    ValueType = "int"
	StringType ValueType = "string"
	JsonType   ValueType = "json"

	Ledger   CacherType = "ledger"
	Ethereum CacherType = "ethereum"
	Waves    CacherType = "waves"
)

type Cacher interface {
	GetLastHeight() (uint64, error)
	GetBlockHash(height uint64) (string, error)
	GetData(height uint64) (map[string]Data, error)
	GetType() CacherType
}

type StartHeightOpt struct {
	Height uint64
}

func Start(cacher Cacher, db *pg.DB, heightInterval uint64, startHeightOpt *StartHeightOpt) {
	for {
		var lastScanHeight uint64
		if startHeightOpt != nil && lastScanHeight == 0 {
			lastScanHeight = startHeightOpt.Height
		} else {
			dataLog := new(DataLog)
			err := db.Model(dataLog).Order("height DESC").Limit(1).Select()
			if err != nil {
				fmt.Printf("Error:%s\n", err.Error())
			}
			lastScanHeight = uint64(dataLog.Height)
		}

		err := scan(cacher, db, heightInterval, lastScanHeight+1)
		if err != nil {
			fmt.Printf("Error:%s\n", err.Error())
		}

		time.Sleep(10 * time.Second)
	}
}

func scan(cacher Cacher, db *pg.DB, heightInterval uint64, lastScanHeight uint64) error {
	lastHeight, err := cacher.GetLastHeight()
	if err != nil {
		return err
	}

	for height := lastScanHeight; height <= lastHeight; height++ {
		fmt.Printf("Scan height (%s):%d\n", cacher.GetType(), height)
		blockHash, err := cacher.GetBlockHash(height)
		if err != nil {
			return err
		}
		err = insertData(cacher, db, height, blockHash)
		if err != nil {
			return err
		}
	}

	for height := lastHeight - heightInterval; height <= lastHeight; height++ {
		fmt.Printf("Rescan height (%s):%d\n", cacher.GetType(), height)
		blockHash, err := cacher.GetBlockHash(height)
		if err != nil {
			return err
		}
		err = insertData(cacher, db, height, blockHash)
		if err != nil {
			return err
		}

		_, err = db.Model(&DataLog{}).Where("height = ?", int(height)).Where("block != ?", blockHash).Where("chain = ?", string(cacher.GetType())).Delete()
		if err != nil {
			return err
		}
	}

	return nil
}

func insertData(cacher Cacher, db *pg.DB, height uint64, blockHash string) error {
	data, err := cacher.GetData(height)
	if err != nil {
		return err
	}

	for k, v := range data {
		var value string
		switch v.Type {
		case IntType:
			value = fmt.Sprintf("%d", v.Value.(uint64))
		case StringType:
			value = v.Value.(string)
		case JsonType:
			value = string(v.Value.([]byte))
		}

		find := new(DataLog)

		db.Model(find).Where("key != ?", k).Where("height = ?", int(height)).Where("block = ?", blockHash).Where("chain = ?", string(cacher.GetType())).Limit(1).Select()
		if find.Key != "" {
			continue
		}
		err := db.Insert(&DataLog{
			BlockHash: blockHash,
			Value:     value,
			Key:       k,
			Type:      string(v.Type),
			Height:    int(height),
			ChainType: string(cacher.GetType()),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

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
	BlockHash string `pg:"blockHash"`
	Key       string `pg:"key"`
	Type      string `pg:"type"`
	Value     string `pg:"value"`
	Height    uint64 `pg:"height"`
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
				fmt.Printf("Error:%s", err.Error())
			}
			lastScanHeight = dataLog.Height
		}

		err := scan(cacher, db, heightInterval, lastScanHeight)
		if err != nil {
			fmt.Printf("Error:%s", err.Error())
		}

		time.Sleep(10 * time.Second)
	}
}

func scan(cacher Cacher, db *pg.DB, heightInterval uint64, lastScanHeight uint64) error {
	lastHeight, err := cacher.GetLastHeight()
	if err != nil {
		return err
	}

	for height := lastHeight; height <= lastScanHeight; height++ {
		blockHash, err := cacher.GetBlockHash(height)
		if err != nil {
			return err
		}
		err = getData(cacher, db, height, blockHash)
		if err != nil {
			return err
		}
	}

	for height := lastScanHeight - heightInterval; height <= lastScanHeight; height++ {
		blockHash, err := cacher.GetBlockHash(height)
		if err != nil {
			return err
		}
		err = getData(cacher, db, height, blockHash)
		if err != nil {
			return err
		}

		_, err = db.Model(&DataLog{}).Where("height", height).Where("blockHash != ?", blockHash).Delete()
		if err != nil {
			return err
		}
	}

	return nil
}

func getData(cacher Cacher, db *pg.DB, height uint64, blockHash string) error {
	data, err := cacher.GetData(height)
	if err != nil {
		return err
	}

	var dataLogs []DataLog
	for k, v := range data {
		var value string
		switch v.Type {
		case IntType:
			value = fmt.Sprintf("%d", v.Value.(int64))
		case StringType:
			value = v.Value.(string)
		case JsonType:
			value = string(v.Value.([]byte))
		}

		dataLogs = append(dataLogs, DataLog{
			BlockHash: blockHash,
			Value:     value,
			Key:       k,
			Type:      string(v.Type),
			Height:    height,
		})

		err := db.Insert(db)
		if err != nil {
			return err
		}
	}

	return nil
}

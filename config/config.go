package config

import (
	"cache-updater/cacher"
	"encoding/json"
	"io/ioutil"
)

type DB struct {
	Addr     string
	User     string
	Password string
	Database string
}

type Chain struct {
	IntervalHeight uint64
	Host           string
}
type Config struct {
	DB
	Chains map[string]Chain

	Nebulae map[cacher.CacherType][]string
}

func Load(filename string) (Config, error) {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}
	config := Config{}
	if err := json.Unmarshal(file, &config); err != nil {
		return Config{}, err
	}
	return config, err
}

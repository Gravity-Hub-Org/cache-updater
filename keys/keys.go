package keys

import (
	"strings"
)

const (
	Separator string = "_"

	PulseKey   string = "pulse"
	ConsulsKey string = "consuls"
	OraclesKey string = "oracles"
	ScoresKey  string = "scores"
)

func FormConsuls() string {
	return ConsulsKey
}

func FormScores(validator string) string {
	return strings.Join([]string{ScoresKey, validator}, Separator)
}

func FormOraclesByNebula(nebula string) string {
	return strings.Join([]string{OraclesKey, nebula}, Separator)
}

func FormPulse(nebulaId string, height string) string {
	return strings.Join([]string{PulseKey, nebulaId, height}, Separator)
}

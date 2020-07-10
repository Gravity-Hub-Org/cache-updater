package keys

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type Key string

const (
	Separator string = "_"

	ConsulsKey           Key = "consuls"
	PrevConsulsKey       Key = "prev_consuls"
	ConsulsSignKey       Key = "consuls_sing"
	OraclesSignNebulaKey Key = "oracles_sign"
)

func FormPrevConsulsKey() string {
	return string(PrevConsulsKey)
}

func FormOraclesSignNebulaKey(validatorAddress []byte, nebulaId []byte, roundId int64) string {
	return strings.Join([]string{string(OraclesSignNebulaKey), hexutil.Encode(validatorAddress), hexutil.Encode(nebulaId), fmt.Sprintf("%d", roundId)}, Separator)
}

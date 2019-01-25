package sqlsmith

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

var typeNames = func() map[string]types.T {
	m := map[string]types.T{}
	for _, T := range types.OidToType {
		m[T.SQLName()] = T
	}
	return m
}()

func typeFromName(name string) types.T {
	typ, ok := typeNames[strings.ToLower(name)]
	if !ok {
		panic(fmt.Errorf("unknown type name: %s", name))
	}
	return typ
}

func getRandType() types.T {
	arr := types.AnyNonArray
	return arr[rand.Intn(len(arr))]
}

package sqlsmith

import (
	"fmt"
	"math/rand"
)

type sqlType int

const (
	anyType sqlType = iota
	intType
	boolType
	stringType
)

func oidToType(oid int) (sqlType, bool) {
	switch oid {
	case 20:
		return intType, true
	case 16:
		return boolType, true
	case 25:
		return stringType, true
	}
	return 0, false
}

func typeName(typ sqlType) string {
	switch typ {
	case anyType:
		return "<any>"
	case intType:
		return "int"
	case boolType:
		return "bool"
	case stringType:
		return "string"
	default:
		panic("unhandled type")
	}
}

func typeFromName(name string) sqlType {
	switch name {
	case "INT":
		return intType
	case "STRING":
		return stringType
	case "BOOL":
		return boolType
	default:
		panic(fmt.Sprintf("unhandled type %q", name))
	}
}

var types = []sqlType{intType, boolType, stringType}

func getType() sqlType {
	return types[rand.Intn(len(types))]
}


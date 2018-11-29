package main

import "math/rand"

const (
	anyBaseType baseType = iota
	intBaseType
	boolBaseType
	stringBaseType
)

type sqlType struct {
	base     baseType
	nullable bool
}

var anyType = sqlType{anyBaseType, true}
var intType = sqlType{intBaseType, true}
var stringType = sqlType{stringBaseType, true}
var boolType = sqlType{boolBaseType, true}
var anyNotNullType = sqlType{anyBaseType, false}
var intNotNullType = sqlType{intBaseType, false}
var stringNotNullType = sqlType{stringBaseType, false}
var boolNotNullType = sqlType{boolBaseType, false}

func oidToType(oid int) (baseType, bool) {
	switch oid {
	case 20:
		return intBaseType, true
	case 16:
		return boolBaseType, true
	case 25:
		return stringBaseType, true
	}
	return 0, false
}

func typeName(typ sqlType) string {
	switch typ.base {
	case anyBaseType:
		return "<any>"
	case intBaseType:
		return "int"
	case boolBaseType:
		return "bool"
	case stringBaseType:
		return "string"
	default:
		panic("unhandled type")
	}
}

func typeFromName(name string, nullable bool) sqlType {
	var t sqlType
	switch name {
	case "INT":
		t.base = intBaseType
	case "STRING":
		t.base = stringBaseType
	case "BOOL":
		t.base = boolBaseType
	}

	t.nullable = nullable
	return t
}

var types = []sqlType{intType, boolType, stringType}

func getType() sqlType {
	return types[rand.Intn(len(types))]
}


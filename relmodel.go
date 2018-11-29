package main

import (
	"bytes"
	"fmt"
)

type baseType int

type writability int

const (
	notWritable writability = iota
	writable
	mustBeWritten
)

type column struct {
	name        string
	typ         sqlType
	writability writability
}

type relation struct {
	cols []column
}

type namedRelation struct {
	relation
	name string
}

type aliasedRelation struct {
	// ??
}

type table struct {
	namedRelation
	schema       string
	isInsertable bool
	isBaseTable  bool
	constraints  []string
}

type scope struct {
	parent *scope
	level  int
	tables []namedRelation
	refs   []tableRef
	count  *int
	schema *schema
	expr relExpr

	out bytes.Buffer
	// schema *schema
}

func (s *scope) push() *scope {
	return &scope{
		parent: s,
		level:  s.level + 1,
		tables: s.tables,
		refs:   append(make([]tableRef, 0, len(s.refs)), s.refs...),
		count:  s.count,
		schema: s.schema,
	}
}

func (s *scope) name(prefix string) string {
	*s.count++
	return fmt.Sprintf("%s_%d", prefix, *s.count)
}

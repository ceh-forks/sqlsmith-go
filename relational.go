package main

import (
	"bytes"
	"fmt"
	"math/rand"
)

func (s *scope) makeStmt() (*scope, bool) {
	if d6() < 3 {
		return s.makeInsert()
	}
	return s.makeReturningStmt(nil)
}

func (s *scope) makeReturningStmt(desiredTypes []sqlType) (*scope, bool) {
	for i := 0; i < retryCount; i++ {
		var outScope *scope
		var ok bool
		if s.level < d6() && d6() < 3{
			outScope, ok = s.makeSetOp(desiredTypes)
		} else if d6() < 3 {
			outScope, ok = s.makeValues(desiredTypes)
		} else {
			outScope, ok = s.makeSelect(desiredTypes)
		}
		if ok {
			return outScope, true
		}
	}
	return nil, false
}

type Format interface {
	Format(*bytes.Buffer)
}

type valueExpr interface {
	Format
	Type() sqlType
}

// REL OPS

type relExpr interface {
	Format

	Cols() []column
}

type tableRef interface {
	relExpr

	Name() string
	Refs() []tableRef
}

////////
// TABLE
////////

type tableExpr struct {
	alias string
	rel   namedRelation
}

func (t tableExpr) Name() string {
	return t.alias
}

func (t tableExpr) Format(buf *bytes.Buffer) {
	fmt.Fprintf(buf, "%s as %s", t.rel.name, t.alias)
}

func (t tableExpr) Cols() []column {
	return t.rel.cols
}

func (t tableExpr) Refs() []tableRef {
	return []tableRef{t}
}

///////
// JOIN
///////

type join struct {
	lhs  relExpr
	rhs  relExpr
	on   valueExpr
	cols []column

	alias string
}

func (s *scope) makeJoinExpr() (*scope, bool) {
	outScope := s.push()
	leftScope, ok := s.makeDataSource()
	if !ok {
		return nil, false
	}
	rightScope, ok := s.makeDataSource()
	if !ok {
		return nil, false
	}

	lhs := leftScope.expr
	rhs := rightScope.expr

	cols := make([]column, 0)
	lCols := lhs.Cols()
	for _, c := range lCols {
		cols = append(cols, c)
	}
	rCols := rhs.Cols()
	for _, c := range rCols {
		cols = append(cols, c)
	}

	outScope.refs = append(outScope.refs, leftScope.refs...)
	outScope.refs = append(outScope.refs, rightScope.refs...)
	on, ok := s.makeBoolExpr()
	if !ok {
		return nil, false
	}

	outScope.expr = &join{
		lhs:   lhs,
		rhs:   rhs,
		cols:  cols,
		on:    on,
		alias: s.name("tab"),
	}

	return outScope, true
}

func (j *join) Format(buf *bytes.Buffer) {
	j.lhs.Format(buf)
	buf.WriteString(" join ")
	j.rhs.Format(buf)
	buf.WriteString(" on ")
	j.on.Format(buf)
}

func (j *join) Cols() []column {
	return j.cols
}

// STATEMENTS

type statement interface {
	Format
}

/////////
// SELECT
/////////

type selectExpr struct {
	fromClause []relExpr
	selectList []valueExpr
	filter     valueExpr
	limit      string
	distinct   bool
	scope      *scope
	orderBy    []valueExpr
}

func (s *selectExpr) Format(buf *bytes.Buffer) {
	buf.WriteString("select ")
	if s.distinct {
		buf.WriteString("distinct ")
	}
	comma := ""
	for _, v := range s.selectList {
		buf.WriteString(comma)
		v.Format(buf)
		comma = ", "
	}
	buf.WriteString(" from ")
	comma = ""
	for _, v := range s.fromClause {
		buf.WriteString(comma)
		v.Format(buf)
		comma = ", "
	}

	if s.filter != nil {
		buf.WriteString(" where ")
		s.filter.Format(buf)
	}

	if s.orderBy != nil {
		buf.WriteString(" order by ")
		comma = ""
		for _, v := range s.orderBy {
			buf.WriteString(comma)
			v.Format(buf)
			comma = ", "
		}
	}

	if s.limit != "" {
		buf.WriteString(" ")
		buf.WriteString(s.limit)
	}
}

func (s *scope) makeSelect(desiredTypes []sqlType) (*scope, bool) {
	outScope := s.push()

	var out selectExpr
	out.fromClause = []relExpr{}
	{
		fromScope, ok := s.makeDataSource()
		if !ok {
			return nil, false
		}
		out.fromClause = append(out.fromClause, fromScope.expr)
		outScope = fromScope
	}

	selectList, ok := outScope.makeSelectList(desiredTypes)
	if !ok {
		return nil, false
	}

	out.selectList = selectList

	if coin() {
		out.filter, ok = outScope.makeBoolExpr()
		if !ok {
			return nil, false
		}
	}

	for coin() {
		expr, ok := outScope.makeScalar(anyType)
		if !ok {
			return nil, false
		}
		out.orderBy = append(out.orderBy, expr)
	}

	out.distinct = d100() == 1

	if d6() > 2 {
		out.limit = fmt.Sprintf("limit %d", d100())
	}

	outScope.expr = &out

	return outScope, true
}

func (s *scope) makeSelectList(desiredTypes []sqlType) ([]valueExpr, bool) {
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getType())
			if d6() == 1 {
				break
			}
		}
	}
	var result []valueExpr
	for _, t := range desiredTypes {
		next, ok := s.makeScalar(t)
		if !ok {
			return nil, false
		}
		result = append(result, next)
	}
	return result, true
}

func (s *selectExpr) Name() string {
	return ""
}

func (s *selectExpr) Refs() []tableRef {
	return nil
}

func (s *selectExpr) Cols() []column {
	return nil
}

func (s *scope) getTableExpr() *scope {
	outScope := s.push()
	table := s.tables[rand.Intn(len(s.tables))]
	outScope.expr = &tableExpr{
		rel:   table,
		alias: s.name("tab"),
	}
	return outScope
}

func (s *scope) makeDataSource() (*scope, bool) {
	if s.level < 3+d6() {
		if d6() > 4 {
			return s.makeJoinExpr()
		}
	}

	return s.getTableExpr(), true
}

/////////
// INSERT
/////////

type insert struct {
	target  string
	targets []column
	input   statement
}

func (i *insert) Format(buf *bytes.Buffer) {
	buf.WriteString("insert into ")
	buf.WriteString(i.target)
	buf.WriteString(" (")
	comma := ""
	for _, c := range i.targets {
		buf.WriteString(comma)
		buf.WriteString(c.name)
		comma = ", "
	}
	buf.WriteString(") ")
	i.input.Format(buf)
}

func (s *scope) makeInsert() (*scope, bool) {
	outScope := s.push()
	target := s.getTableExpr().expr.(*tableExpr)

	desiredTypes := make([]sqlType, 0)
	targets := make([]column, 0)
	for _, c := range target.Cols() {
		if c.writability == writable && (!c.typ.nullable || coin()) {
			targets = append(targets, c)
			desiredTypes = append(desiredTypes, c.typ)
		}
	}

	input, ok := s.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	outScope.expr = &insert{
		target:  target.rel.name,
		targets: targets,
		input:   input.expr,
	}

	return outScope, true
}

func (i *insert) Name() string {
	return ""
}

func (i *insert) Refs() []tableRef {
	return nil
}

func (i *insert) Cols() []column {
	return nil
}

/////////
// VALUES
/////////

type values struct {
	values [][]valueExpr
}

func (s *scope) makeValues(desiredTypes []sqlType) (*scope, bool) {
	outScope := s.push()
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getType())
			if d6() < 2 {
				break
			}
		}
	}

	count := rand.Intn(5) + 1
	vals := make([][]valueExpr, 0, count)
	for i := 0; i < count; i++ {
		tuple := make([]valueExpr, 0, len(desiredTypes))
		for _, t := range desiredTypes {
			e, ok := outScope.makeScalar(t)
			if !ok {
				return nil, false
			}
			tuple = append(tuple, e)
		}
		vals = append(vals, tuple)
	}

	outScope.expr = &values{vals}
	return outScope, true
}

func (v *values) Name() string {
	return ""
}

func (v *values) Refs() []tableRef {
	return nil
}

func (v *values) Cols() []column {
	return nil
}

func (v *values) Format(buf *bytes.Buffer) {
	buf.WriteString("values ")
	comma := ""
	for _, t := range v.values {
		buf.WriteString(comma)
		buf.WriteByte('(')
		comma2 := ""
		for _, d := range t {
			buf.WriteString(comma2)
			d.Format(buf)
			comma2 = ", "
		}
		buf.WriteByte(')')
		comma = ", "
	}
}

/////////
// SET OP
/////////

type setOp struct {
	op string
	left relExpr
	right relExpr
}

var setOps = []string{"union", "union all", "except", "except all", "intersect", "intersect all"}

func (s *scope) makeSetOp(desiredTypes []sqlType) (*scope, bool) {
	outScope := s.push()
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getType())
			if d6() < 2 {
				break
			}
		}
	}

	leftScope, ok := outScope.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	rightScope, ok := outScope.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	outScope.expr = &setOp{
		op: setOps[rand.Intn(len(setOps))],
		left: leftScope.expr,
		right: rightScope.expr,
	}

	return outScope, true
}

func (s *setOp) Cols() []column {
	return s.left.Cols()
}

func (s *setOp) Format(buf *bytes.Buffer) {
	buf.WriteByte('(')
	s.left.Format(buf)
	buf.WriteByte(')')
	buf.WriteByte(' ')
	buf.WriteString(s.op)
	buf.WriteByte(' ')
	buf.WriteByte('(')
	s.right.Format(buf)
	buf.WriteByte(')')
}

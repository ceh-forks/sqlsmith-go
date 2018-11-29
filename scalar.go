package main

import (
	"bytes"
	"fmt"
	"math/rand"
)

func (s *scope) makeScalar(typ sqlType) (valueExpr, bool) {
	pickedType := typ
	if typ == anyType {
		pickedType = getType()
	}
	s = s.push()

	for i := 0; i < retryCount; i++ {
		var result valueExpr
		var ok bool
		if s.level < d6() && d9() == 1 {
			result, ok = s.makeCaseExpr(pickedType)
		} else if s.level < d6() && d42() == 1 {
			result, ok = s.makeCoalesceExpr(pickedType)
		} else if len(s.refs) > 0 && d20() > 1 {
			result, ok = s.makeColRef(typ)
		} else if s.level < d6() && d9() == 1 {
			result, ok = s.makeBinOp(typ)
			// uncomment when the panic is fixed
			// } else if d6() == 1 {
			// 	result, ok = s.makeScalarSubquery(typ)
		} else {
			result, ok = s.makeConstExpr(pickedType), true
		}
		if ok {
			return result, ok
		}
	}

	// Retried enough times, give up.
	return nil, false
}

func (s *scope) makeBoolExpr() (valueExpr, bool) {
	s = s.push()

	for i := 0; i < retryCount; i++ {
		var result valueExpr
		var ok bool

		if d6() < 4 {
			result, ok = s.makeBinOp(boolType)
		} else if d6() < 4 {
			result, ok = s.makeScalar(boolType)
		} else {
			result, ok = s.makeExists()
		}

		if ok {
			return result, ok
		}
	}

	// Retried enough times, give up.
	return nil, false
}

///////
// CASE
///////

type caseExpr struct {
	condition valueExpr
	trueExpr  valueExpr
	falseExpr valueExpr
}

func (c *caseExpr) Type() sqlType {
	return c.trueExpr.Type()
}

func (c *caseExpr) Format(buf *bytes.Buffer) {
	buf.WriteString("case when ")
	c.condition.Format(buf)
	buf.WriteString(" then ")
	c.trueExpr.Format(buf)
	buf.WriteString(" else ")
	c.falseExpr.Format(buf)
	buf.WriteString(" end")
}

func (s *scope) makeCaseExpr(typ sqlType) (valueExpr, bool) {
	condition, ok := s.makeScalar(boolType)
	if !ok {
		return nil, false
	}

	trueExpr, ok := s.makeScalar(typ)
	if !ok {
		return nil, false
	}

	falseExpr, ok := s.makeScalar(typ)
	if !ok {
		return nil, false
	}

	return &caseExpr{condition, trueExpr, falseExpr}, true
}

///////////
// COALESCE
///////////

type coalesceExpr struct {
	firstExpr  valueExpr
	secondExpr valueExpr
}

func (c *coalesceExpr) Type() sqlType {
	return c.firstExpr.Type()
}

func (c *coalesceExpr) Format(buf *bytes.Buffer) {
	buf.WriteString("cast(coalesce(")
	c.firstExpr.Format(buf)
	buf.WriteString(", ")
	c.secondExpr.Format(buf)
	buf.WriteString(") as ")
	buf.WriteString(typeName(c.firstExpr.Type()))
	buf.WriteString(")")
}

func (s *scope) makeCoalesceExpr(typ sqlType) (valueExpr, bool) {
	firstExpr, ok := s.makeScalar(typ)
	if !ok {
		return nil, false
	}

	secondExpr, ok := s.makeScalar(typ)
	if !ok {
		return nil, false
	}

	return &coalesceExpr{firstExpr, secondExpr}, true
}

////////
// CONST
////////

type constExpr struct {
	typ  sqlType
	expr string
}

func (c *constExpr) Type() sqlType {
	return c.typ
}

func (c *constExpr) Format(buf *bytes.Buffer) {
	buf.WriteString(c.expr)
}

func (s *scope) makeConstExpr(typ sqlType) valueExpr {
	var val string
	if typ.nullable && d6() < 3 {
		val = fmt.Sprintf("null::%s", typeName(typ))
	} else {
		switch typ.base {
		case intBaseType:
			// Small right now because of #32682.
			val = fmt.Sprintf("%d", d6())
		case boolBaseType:
			if coin() {
				val = "true"
			} else {
				val = "false"
			}
		case stringBaseType:
			if coin() {
				val = "'hello'"
			} else {
				val = "'goodbye'"
			}
		default:
			panic("unknown type")
		}
	}
	// TODO: maintain context and see if we're in an INSERT, and maybe use
	// DEFAULT

	return &constExpr{typ, val}
}

/////////
// COLREF
/////////

type colRefExpr struct {
	ref string
	typ sqlType
}

func (c *colRefExpr) Type() sqlType {
	return c.typ
}

func (c *colRefExpr) Format(buf *bytes.Buffer) {
	buf.WriteString(c.ref)
}

func (s *scope) makeColRef(typ sqlType) (valueExpr, bool) {
	ref := s.refs[rand.Intn(len(s.refs))]
	col := ref.Cols()[rand.Intn(len(ref.Cols()))]
	if typ != anyType && col.typ != typ {
		return nil, false
	}

	return &colRefExpr{
		ref: ref.Name() + "." + col.name,
		typ: col.typ,
	}, true
}

/////////
// BIN OP
/////////

type opExpr struct {
	outTyp sqlType

	left  valueExpr
	right valueExpr
	op    string
}

func (o *opExpr) Type() sqlType {
	return o.outTyp
}

func (o *opExpr) Format(buf *bytes.Buffer) {
	buf.WriteByte('(')
	o.left.Format(buf)
	buf.WriteByte(' ')
	buf.WriteString(o.op)
	buf.WriteByte(' ')
	o.right.Format(buf)
	buf.WriteByte(')')
}

func (s *scope) makeBinOp(typ sqlType) (valueExpr, bool) {
	if typ == anyType {
		typ = getType()
	}
	ops := s.schema.operators[typ.base]
	op := ops[rand.Intn(len(ops))]

	left, ok := s.makeScalar(sqlType{base: op.left})
	if !ok {
		return nil, false
	}
	right, ok := s.makeScalar(sqlType{base: op.right})
	if !ok {
		return nil, false
	}

	return &opExpr{
		outTyp: typ,
		left:   left,
		right:  right,
		op:     op.name,
	}, true
}

/////////
// EXISTS
/////////

type exists struct {
	subquery relExpr
}

func (e *exists) Format(buf *bytes.Buffer) {
	buf.WriteString("exists(")
	e.subquery.Format(buf)
	buf.WriteString(")")
}

func (e *exists) Type() sqlType {
	return boolType
}

func (s *scope) makeExists() (valueExpr, bool) {
	outScope, ok := s.makeSelect(nil)
	if !ok {
		return nil, false
	}

	return &exists{outScope.expr}, true
}

//////////////////
// SCALAR SUBQUERY
//////////////////

type scalarSubq struct {
	subquery relExpr
}

func (s *scalarSubq) Format(buf *bytes.Buffer) {
	buf.WriteString("(")
	s.subquery.Format(buf)
	buf.WriteString(")")
}

func (s *scalarSubq) Type() sqlType {
	return s.subquery.Cols()[0].typ
}

func (s *scope) makeScalarSubquery(typ sqlType) (valueExpr, bool) {
	outScope, ok := s.makeSelect([]sqlType{typ})
	if !ok {
		return nil, false
	}

	return &scalarSubq{outScope.expr}, true
}

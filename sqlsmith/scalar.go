package sqlsmith

import (
	"bytes"
	"fmt"
	"math/rand"
)

// makeScalar attempts constructs a scalar expression of the requested type.
// If it was unsuccessful, it will return false.
func (s *scope) makeScalar(typ sqlType) (scalarExpr, bool) {
	pickedType := typ
	if typ == anyType {
		pickedType = getType()
	}
	s = s.push()

	for i := 0; i < retryCount; i++ {
		var result scalarExpr
		var ok bool
		// TODO(justin): this is how sqlsmith chooses what to do, but it feels
		// to me like there should be a more clean/principled approach here.
		if s.level < d6() && d9() == 1 {
			result, ok = s.makeCaseExpr(pickedType)
		} else if s.level < d6() && d42() == 1 {
			result, ok = s.makeCoalesceExpr(pickedType)
		} else if len(s.refs) > 0 && d20() > 1 {
			result, ok = s.makeColRef(typ)
		} else if s.level < d6() && d9() == 1 {
			result, ok = s.makeBinOp(typ)
		} else if s.level < d6() && d9() == 1 {
			result, ok = s.makeFunc(typ)
		} else if s.level < d6() && d6() == 1 {
			result, ok = s.makeScalarSubquery(typ)
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

// TODO(justin): sqlsmith separated this out from the general case for
// some reason - I think there must be a clean way to unify the two.
func (s *scope) makeBoolExpr() (scalarExpr, bool) {
	s = s.push()

	for i := 0; i < retryCount; i++ {
		var result scalarExpr
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
	condition scalarExpr
	trueExpr  scalarExpr
	falseExpr scalarExpr
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

func (s *scope) makeCaseExpr(typ sqlType) (scalarExpr, bool) {
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
	firstExpr  scalarExpr
	secondExpr scalarExpr
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

func (s *scope) makeCoalesceExpr(typ sqlType) (scalarExpr, bool) {
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

func (s *scope) makeConstExpr(typ sqlType) scalarExpr {
	var val string
	if d6() == 1 {
		val = fmt.Sprintf("null::%s", typeName(typ))
	} else {
		switch typ {
		case intType:
			// Small right now because of #32682.
			val = fmt.Sprintf("%d", d6())
		case boolType:
			if coin() {
				val = "true"
			} else {
				val = "false"
			}
		case stringType:
			if coin() {
				val = "'hello'"
			} else {
				val = "'goodbye'"
			}
		default:
			panic("unknown type")
		}
	}
	// TODO(justin): maintain context and see if we're in an INSERT, and maybe use
	// DEFAULT (which is a legal "value" in such a context).

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

func (s *scope) makeColRef(typ sqlType) (scalarExpr, bool) {
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

	left  scalarExpr
	right scalarExpr
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

func (s *scope) makeBinOp(typ sqlType) (scalarExpr, bool) {
	if typ == anyType {
		typ = getType()
	}
	ops := s.schema.GetOperatorsByOutputType(typ)
	op := ops[rand.Intn(len(ops))]

	left, ok := s.makeScalar(op.left)
	if !ok {
		return nil, false
	}
	right, ok := s.makeScalar(op.right)
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

//////////
// FUNC OP
//////////

type funcExpr struct {
	outTyp sqlType

	name string
	inputs []scalarExpr
}

func (f *funcExpr) Type() sqlType {
	return f.outTyp
}

func (f *funcExpr) Format(buf *bytes.Buffer) {
	buf.WriteString(f.name)
	buf.WriteByte('(')
	comma := ""
	for _, a := range f.inputs {
		buf.WriteString(comma)
		a.Format(buf)
		comma = ", "
	}
	buf.WriteByte(')')
}

func (s *scope) makeFunc(typ sqlType) (scalarExpr, bool) {
	if typ == anyType {
		typ = getType()
	}
	ops := s.schema.GetFunctionsByOutputType(typ)
	op := ops[rand.Intn(len(ops))]

	args := make([]scalarExpr, 0)
	for i := range op.inputs {
		in, ok := s.makeScalar(op.inputs[i])
		if !ok {
			return nil, false
		}
		args = append(args, in)
	}

	return &funcExpr{
		outTyp: typ,
		name: op.name,
		inputs: args,
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

func (s *scope) makeExists() (scalarExpr, bool) {
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
	return s.subquery.(*selectExpr).selectList[0].Type()
}

func (s *scope) makeScalarSubquery(typ sqlType) (scalarExpr, bool) {
	outScope, ok := s.makeSelect([]sqlType{typ})
	if !ok {
		return nil, false
	}

	outScope.expr.(*selectExpr).limit = "limit 1"
	return &scalarSubq{outScope.expr}, true
}

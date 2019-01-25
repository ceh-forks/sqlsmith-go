package sqlsmith

import (
	"database/sql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type operator struct {
	name  string
	left  sqlType
	right sqlType
	out   sqlType
}

type function struct {
	name   string
	inputs []sqlType
	out    sqlType
}

// schema represents the state of the database as sqlsmith-go understands it, including
// not only the tables present but also things like what operator overloads exist.
type schema struct {
	tables    []namedRelation
	operators map[sqlType][]operator
	functions map[sqlType][]function
}

func (s *schema) makeScope() *scope {
	return &scope{
		namer:  &namer{make(map[string]int)},
		schema: s,
	}
}

func (s *schema) GetOperatorsByOutputType(outTyp sqlType) []operator {
	return s.operators[outTyp]
}

func (s *schema) GetFunctionsByOutputType(outTyp sqlType) []function {
	return s.functions[outTyp]
}

func makeSchema() *schema {
	db, err := sql.Open("postgres", "port=26257 user=root dbname=defaultdb sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	return &schema{
		tables:    extractTables(db),
		operators: extractOperators(db),
		functions: extractFunctions(db),
	}
}

func extractTables(db *sql.DB) []namedRelation {
	rows, err := db.Query(`
	SELECT
		table_catalog,
		table_schema,
		table_name,
		column_name,
		crdb_sql_type,
		generation_expression != '' AS computed,
		is_nullable = 'YES' AS nullable
	FROM
		information_schema.columns
	WHERE
		table_schema = 'public'
	ORDER BY
		table_catalog, table_schema, table_name
	`)
	// TODO(justin): have a flag that includes system tables?
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// This is a little gross: we want to operate on each segment of the results
	// that corresponds to a single table. We could maybe json_agg the results
	// or something for a cleaner processing step?

	firstTime := true
	var lastCatalog, lastSchema, lastName string
	var tables []namedRelation
	var currentCols []column
	emit := func() {
		tables = append(tables, namedRelation{
			cols: currentCols,
			name: lastName,
		})
	}
	for rows.Next() {
		var catalog, schema, name, col, typ string
		var computed, nullable bool
		rows.Scan(&catalog, &schema, &name, &col, &typ, &computed, &nullable)

		if firstTime {
			lastCatalog = catalog
			lastSchema = schema
			lastName = name
		}
		firstTime = false

		if lastCatalog != catalog || lastSchema != schema || lastName != name {
			emit()
			currentCols = nil
		}

		writability := writable
		if computed {
			writability = notWritable
		}

		currentCols = append(
			currentCols,
			column{
				col,
				typeFromName(typ),
				nullable,
				writability,
			},
		)
		lastCatalog = catalog
		lastSchema = schema
		lastName = name
	}
	if !firstTime {
		emit()
	}
	return tables
}

func extractOperators(db *sql.DB) map[sqlType][]operator {
	rows, err := db.Query(`
SELECT
	oprname, oprleft, oprright, oprresult
FROM
	pg_catalog.pg_operator
WHERE
	0 NOT IN (oprresult, oprright, oprleft)
`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	result := make(map[sqlType][]operator, 0)
	for rows.Next() {
		var name string
		var left, right, out int
		rows.Scan(&name, &left, &right, &out)
		leftTyp, ok := oidToType(left)
		if !ok {
			continue
		}
		rightTyp, ok := oidToType(right)
		if !ok {
			continue
		}
		outTyp, ok := oidToType(out)
		if !ok {
			continue
		}
		result[outTyp] = append(
			result[outTyp],
			operator{
				name:  name,
				left:  leftTyp,
				right: rightTyp,
				out:   outTyp,
			},
		)
	}
	return result
}

func extractFunctions(db *sql.DB) map[sqlType][]function {
	rows, err := db.Query(`
SELECT
	proname, proargtypes::INT[], prorettype
FROM
	pg_catalog.pg_proc
WHERE
	NOT proisagg
	AND NOT proiswindow
	AND NOT proretset
	AND proname NOT IN ('crdb_internal.force_panic', 'crdb_internal.force_log_fatal')
`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	result := make(map[sqlType][]function, 0)
	for rows.Next() {
		var name string
		var inputs []int64
		var returnType int64
		rows.Scan(&name, pq.Array(&inputs), &returnType)

		types := make([]sqlType, len(inputs))
		unsupported := false
		for i, oid := range inputs {
			t, ok := oidToType(int(oid))
			if !ok {
				unsupported = true
				break
			}
			types[i] = t
		}

		if unsupported {
			continue
		}

		out, ok := oidToType(int(returnType))
		if !ok {
			continue
		}

		result[out] = append(result[out], function{
			name,
			types,
			out,
		})
	}
	return result
}

package main

import (
	"database/sql"
	_ "github.com/lib/pq"
)

type operator struct {
	name string
	left baseType
	right baseType
	out baseType
}

type schema struct {
	tables []namedRelation
	operators map[baseType][]operator
}

func (s *schema) makeScope() *scope {
	count := 0
	return &scope{
		count: &count,
		tables: s.tables,
		schema: s,
	}
}

func makeSchema() *schema {
	db, err := sql.Open("postgres", "port=26257 user=root dbname=defaultdb sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	tables := extractTables(db)
	operators := extractOperators(db)

	return &schema{
		tables: tables,
		operators: operators,
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
	// TODO: have a flag that doesn't use system tables.
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	firstTime := true
	var lastCatalog, lastSchema, lastName string
	var tables []namedRelation
	var currentCols []column
	emit := func() {
		tables = append(tables, namedRelation{
			relation: relation{
				cols: currentCols,
			},
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
				typeFromName(typ, nullable),
				writability,
			},
		)
		lastCatalog = catalog
		lastSchema = schema
		lastName = name
	}
	emit()
	return tables
}

func extractOperators(db *sql.DB) map[baseType][]operator {
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

	result := make(map[baseType][]operator, 0)
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
				name: name,
				left: leftTyp,
				right: rightTyp,
				out: outTyp,
			},
		)
	}
	return result
}

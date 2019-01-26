package sqlsmith

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// sqlsmith-go
//
// sqlsmith-go is a random SQL query generator, based off of sqlsmith:
//
//   https://github.com/anse1/sqlsmith
//
// You can think of it as walking a randomly generated AST and materializing
// that AST as it goes, which it then feeds into Cockroach with the hopes of
// finding panics.
//
// However, naively generating such an AST will only find certain kinds of
// panics: they're almost guaranteed not to pass semantic analysis, and so
// any components of the system beyond that will probably not be tested.
// To get around this, sqlsmith tracks scopes and types, very similar to
// how the optbuilder works, to create ASTs which will likely pass
// semantic analysis.
//
// It does this by building the tree top-down. Every level of the tree
// requests input of a certain form. For instance, a SELECT will request
// a list of projections which respect the scope that the SELECT introduces,
// and a function call will request an input value of a particular type,
// subject to the same scope it has. This raises a question: what if we
// are unable to construct an expression meeting the restrictions requested
// by the parent expression? Rather than do some fancy constraint solving
// (which could be an interesting direction for this tool to go in the
// future, but I've found to be difficult when I've tried in the past)
// sqlsmith will simply try randomly to generate an expression, and once
// it fails a certain number of times, it will retreat up the tree and
// retry at a higher level.

const retryCount = 20

func Run() {
	rand.Seed(int64(time.Now().Nanosecond()))

	db, _ := sql.Open("postgres", "port=26257 user=root dbname=defaultdb sslmode=disable")
	defer db.Close()

	schema := makeSchema(db)

	for i := 0; ; i++ {
		if i%100 == 0 {
			create := sqlbase.RandCreateTable(schema.rnd, schema.rnd.Int())
			stmt := create.String()
			fmt.Println(stmt)
			if _, err := db.Exec(stmt); err != nil {
				fmt.Println("error:", err)
			}
			schema.ReloadSchemas()
		}

		s := schema.makeScope()
		sc, ok := s.makeStmt()
		if !ok {
			continue
		}
		expr := sc.expr
		var buf bytes.Buffer
		expr.Format(&buf)
		fmt.Println(buf.String())
		fmt.Println()
		rows, err := db.Query(buf.String())
		if err != nil {
			if strings.Contains(err.Error(), "connection refused") {
				// TODO(justin): we should dump the schema we used along with the panicking query in this case.
				fmt.Println("panic!")
				break
			}
			fmt.Println()
			fmt.Println("error:", err)
			fmt.Println()
		}
		if err == nil {
			_ = rows.Close()
		}
	}
}

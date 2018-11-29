package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

func main() {
	rand.Seed(int64(time.Now().Nanosecond()))

	schema := makeSchema()
	s := schema.makeScope()

	db, _ := sql.Open("postgres", "port=26257 user=root dbname=defaultdb sslmode=disable")
	defer db.Close()
	for i := 0; i < 10000000; i++ {
		sc, ok := s.makeStmt()
		if !ok {
			continue
		}
		expr := sc.expr
		var buf bytes.Buffer
		expr.Format(&buf)
		fmt.Println(buf.String())
		rows, err := db.Query(buf.String())
		if err != nil {
			if strings.Contains(err.Error(), "connection refused") {
				fmt.Println("panic!")
				break
			}
			fmt.Println()
			fmt.Println("error: ", err)
			fmt.Println()
		}
		if err == nil {
			_ = rows.Close()
		}
		*s.count = 0
	}
}

const retryCount = 20

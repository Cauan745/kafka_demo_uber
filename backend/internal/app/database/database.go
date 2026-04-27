// Package appdatabase
package appdatabase

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func Start(port int, database string, host string, user string, password string) *sql.DB {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, password, host, port, database)

	fmt.Println(connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	return db
}

// Package appdatabase
package appdatabase

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type Config struct {
	port     int
	database string
	host     string
	user     string
	password string
}

type Database struct {
	config Config
	DB     *sql.DB
}

func New(port int, database string, host string, user string, password string) *Database {
	config := Config{port, database, host, user, password}

	db := Database{config, config.Start()}

	return &db
}

func (c Config) Start() *sql.DB {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", c.user, c.password, c.host, c.port, c.database)

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

func (db *Database) CreateTables() {
	db.CreateUserTable()
	db.CreateRidesTable()
}

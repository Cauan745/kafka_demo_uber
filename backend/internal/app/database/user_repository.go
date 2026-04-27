package appdatabase

import (
	"log"
)

type User struct {
	Name     string `json:name`
	Password string `json:password`
}

func (db *Database) CreateUserTable() {
	query := `CREATE TABLE IF NOT EXISTS user (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100),
		password VARCHAR(100)
	)`

	_, err := db.DB.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
}

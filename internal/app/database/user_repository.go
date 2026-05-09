package appdatabase

import (
	"fmt"
	"log"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

type User struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

func (db *Database) CreateUserTable() {
	query := `CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) UNIQUE,
		password VARCHAR(100)
	);`

	_, err := db.DB.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *Database) Register(u User) (int, error) {
	if strings.TrimSpace(u.Name) == "" || strings.TrimSpace(u.Password) == "" {
		return -1, fmt.Errorf("name and password can't be empty")
	}

	query := `INSERT INTO users(name, password) 
	values ($1, $2) RETURNING id`

	hash, err := HashPassword(u.Password)
	if err != nil {
		return -1, err
	}

	var pk int

	err = db.DB.QueryRow(query, u.Name, hash).Scan(&pk)
	if err != nil {
		return -1, err
	}

	return pk, nil
}

func (db *Database) Login(u User) (int, error) {
	if strings.TrimSpace(u.Name) == "" || strings.TrimSpace(u.Password) == "" {
		return -1, fmt.Errorf("name and password can't be empty")
	}

	query := `SELECT id, password FROM users u WHERE u.name = $1`

	var pk int
	var hash string

	err := db.DB.QueryRow(query, u.Name).Scan(&pk, &hash)
	if err != nil {
		return -1, err
	}

	res := CheckPasswordHash(u.Password, hash)

	if !res {
		return -1, fmt.Errorf("invalid password")
	}

	return pk, nil
}

func HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes), err
}

func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

package appdatabase

import (
	"log"
)

func (db *Database) CreateRidesTable() {
	query := `CREATE TABLE IF NOT EXISTS rides (
		id SERIAL PRIMARY KEY,
		passenger_id VARCHAR(100),
		driver_id VARCHAR(100),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		finished_at TIMESTAMP
	);`

	_, err := db.DB.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *Database) NewRide(passengerId string, driverId string) (int, error) {
	query := `INSERT INTO rides(passenger_id, driver_id) 
	VALUES ($1, $2) RETURNING id`

	var pk int

	err := db.DB.QueryRow(query, passengerId, driverId).Scan(&pk)
	if err != nil {
		return -1, err
	}

	return pk, nil
}

func (db *Database) FinishRide(id int) error {
	query := `UPDATE rides SET finished_at = CURRENT_TIMESTAMP WHERE id = $1`
	_, err := db.DB.Exec(query, id)
	return err
}

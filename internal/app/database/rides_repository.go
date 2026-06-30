package appdatabase

import (
	"database/sql"
	"log"
	"time"
)

type Ride struct {
	ID          int
	PassengerID string
	DriverID    sql.NullString
	CreatedAt   time.Time
	FinishedAt  sql.NullTime
	DeletedAt   sql.NullTime
}

func (db *Database) CreateRidesTable() {
	query := `CREATE TABLE IF NOT EXISTS rides (
		id SERIAL PRIMARY KEY,
		passenger_id VARCHAR(100),
		driver_id VARCHAR(100),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		finished_at TIMESTAMP,
		deleted_at TIMESTAMP
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

func (db *Database) SetRideDriver(id int, driverId string) error {
	query := `UPDATE rides SET driver_id = $2 WHERE id = $1 AND (driver_id IS NULL OR driver_id = '')`
	_, err := db.DB.Exec(query, id, driverId)
	return err
}

func (db *Database) GetRidesByPassengerId(passengerId string) ([]Ride, error) {
	query := `SELECT id, passenger_id, driver_id, created_at, finished_at, deleted_at FROM rides WHERE passenger_id = $1 AND deleted_at IS NULL ORDER BY created_at DESC`
	rows, err := db.DB.Query(query, passengerId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rides []Ride
	for rows.Next() {
		var r Ride
		err := rows.Scan(&r.ID, &r.PassengerID, &r.DriverID, &r.CreatedAt, &r.FinishedAt, &r.DeletedAt)
		if err != nil {
			return nil, err
		}
		rides = append(rides, r)
	}
	return rides, nil
}

func (db *Database) SoftDeleteRide(id int, passengerId string) error {
	query := `UPDATE rides SET deleted_at = CURRENT_TIMESTAMP WHERE id = $1 AND passenger_id = $2`
	_, err := db.DB.Exec(query, id, passengerId)
	return err
}

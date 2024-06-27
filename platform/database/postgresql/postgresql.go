package postgresql

import (
	"context"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const ConnString = "postgres://root:exevipvl@hostt:5432/EXE201?sslmode=disable"

var db *sqlx.DB

func NewContext() (context.Context, context.CancelFunc) {
	// return context.WithTimeout(context.Background(), 10*time.Second)
	return context.TODO(), nil
}

func initPostgres() {
	var err error
	db, err = sqlx.Connect("postgres", ConnString)
	if err != nil {
		log.Fatalf("[POSTGRES_DB] Cannot connect to PostgresDB %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Cannot ping PostgresDB: %v", err)
	}

	log.Println("[POSTGRES] connect successfully")
}

func init() {
	initPostgres()
}

func PostgresShutdown() {
	_, cancel := NewContext()
	defer cancel()

	if db != nil {
		err := db.Close()
		if err != nil {
			log.Fatalf("Cannot disconnect PostgresDB: %v", err)
		}
	}

	log.Println("[POSTGRES_DB] disconnect successfully")
}

func PostgresDB() *sqlx.DB {
	if db == nil {
		initPostgres()
	}
	return db
}

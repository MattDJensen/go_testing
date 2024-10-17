package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/joho/godotenv"
	_ "github.com/microsoft/go-mssqldb" // registers sql driver
)

func main() {

	df1 := dataframe.New(
		series.New([]string{"a", "b", "c"}, series.String, "col1"),
		series.New([]int{1, 2, 3}, series.Int, "col2"),
		series.New([]float64{1.1, 2.2, 3.3}, series.Float, "col3"),
	)
	fmt.Println(df1)

	db, err := connectToSQLServer()
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	queryTest(db)
	elapsed := time.Since(start)
	fmt.Println("Query took: ", elapsed)

}

func connectToSQLServer() (*sql.DB, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, err
	}
	server := os.Getenv("server")
	port := 1433
	user := os.Getenv("user")
	password := os.Getenv("password")
	database := os.Getenv("database")
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", server, user, password, port, database)
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type Job struct {
	DetailKey int
	JobName   string
}

func queryTest(db *sql.DB) {
	query := "Select DetailKey,JobName from HourlyTime LEFT JOIN Job on HourlyTime.JobKey = Job.JobKey"
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {

		var job Job
		err := rows.Scan(&job.DetailKey, &job.JobName)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(job)

	}
}

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/pinpt/go-drill"
)

func main() {
	conn, err := sql.Open("drill", "http://localhost")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.QueryContext(context.Background(), "SELECT full_name, position_title, salary FROM cp.`employee.json` order by salary desc LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("columns:", cols)
	defer rows.Close()
	for rows.Next() {
		var name, title string
		var salary float32
		if err := rows.Scan(&name, &title, &salary); err != nil {
			log.Fatal(err)
		}
		fmt.Println("name:", name, "title:", title, "salary", salary)
	}
}

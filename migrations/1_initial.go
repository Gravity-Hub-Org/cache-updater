package main

import (
	"fmt"

	"github.com/go-pg/migrations"
)

func init() {

	tableName := "data_logs"

	migrations.MustRegisterTx(
		func(db migrations.DB) error {
			fmt.Printf("creating %v table...\n", tableName)
			_, err := db.Exec(fmt.Sprintf(
				`
				CREATE TABLE %s (
					blockHash text,
					key text,
					type text,
					value text,
					height bigint
				);
				`, tableName))
			return err
		},
		func(db migrations.DB) error {
			fmt.Printf("dropping %v table...\n", tableName)
			_, err := db.Exec(fmt.Sprintf(`DROP TABLE %v;`, tableName))

			return err
		},
	)
}

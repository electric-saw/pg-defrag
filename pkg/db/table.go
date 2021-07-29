package db

import "context"

func (pg *PgConnection) GetSchemaOfTable(ctx context.Context, table string) (string, error) {
	qry := `select schemaname from pg_catalog.pg_tables	where tablename = $1`
	var schema string
	err := pg.Conn.QueryRow(ctx, qry, table).Scan(&schema)
	return schema, err
}

package pg

import (
	"context"
	"database/sql"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"populatr/internal/datasource"
	"populatr/internal/domain"
	"populatr/internal/logger"
	"populatr/internal/retry"
)

const findAllTablesQuery = `
with relations AS (SELECT kcu.table_name,
                          kcu.column_name,
                          ccu.table_name  as foreign_table,
                          ccu.column_name as foreign_column,
                          kcu.table_schema,
                          kcu.table_catalog
                   FROM information_schema.table_constraints tc
                            JOIN information_schema.key_column_usage kcu
                                 ON tc.constraint_name = kcu.constraint_name
                                     AND tc.constraint_schema = kcu.constraint_schema
                                     AND tc.constraint_catalog = kcu.constraint_catalog
                            JOIN information_schema.constraint_column_usage ccu
                                 ON kcu.constraint_name = ccu.constraint_name
                                     and kcu.constraint_schema = ccu.constraint_schema
                                     and kcu.constraint_catalog = ccu.constraint_catalog
                   WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
                     AND tc.constraint_type = 'FOREIGN KEY')

SELECT c.table_name,
       c.column_name,
       r.foreign_table,
       r.foreign_column,
       c.data_type,
       c.character_maximum_length,
       c.numeric_precision,
       c.numeric_scale,
       c.datetime_precision,
       c.is_nullable,
       c.ordinal_position
FROM information_schema.columns c
         LEFT JOIN relations r
                   ON c.table_name = r.table_name
                       AND c.column_name = r.column_name
                        AND c.table_schema = r.table_schema
                        AND c.table_catalog = r.table_catalog
WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY c.table_name, c.ordinal_position, foreign_table DESC, foreign_column DESC;
`

type Manager struct {
	conn datasource.Connection
}

func NewManager(conn datasource.Connection) *Manager {
	return &Manager{
		conn: conn,
	}
}

func (m *Manager) Connect() (*sqlx.DB, error) {
	sslMode := "disable"
	if m.conn.Options.UseSsl {
		sslMode = "enable"
	}
	pgOptions := "host=%s port=%d user=%s password=%s dbname=%s sslmode=%s"
	connStr := fmt.Sprintf(pgOptions, m.conn.Options.Host, m.conn.Options.Port,
		m.conn.Options.Username, m.conn.Options.Password, m.conn.Options.Db, sslMode)
	lgr.Debug().
		Str("connection", connStr)

	return sqlx.Connect(m.conn.Options.Dialect, connStr)
}

type InfoSchemaRepo struct {
	db *sqlx.DB
}

func NewInfoSchemaRepo(db *sqlx.DB) *InfoSchemaRepo {
	return &InfoSchemaRepo{db: db}
}

var lgr = logger.Get()

func (r *InfoSchemaRepo) FindAllMetadata(ctx context.Context) ([]domain.ColumnMetadata, error) {
	var cols []domain.ColumnMetadata
	err := r.db.SelectContext(ctx, &cols, findAllTablesQuery)
	if err != nil {
		return nil, err
	}
	return cols, nil
}

func (r *InfoSchemaRepo) CreateTestData(ctx context.Context, tbls *[]string, ids *[][]string, vals *[][][]any) error {
	for i, tbl := range *tbls {
		query := sq.Insert(tbl).Columns((*ids)[i]...)
		for _, row := range (*vals)[i] {
			query = query.Values(row...)
		}
		query = query.PlaceholderFormat(sq.Dollar)

		stmt, args, err := query.ToSql()
		if err != nil {
			lgr.Warn().
				Err(err).
				Msg("could not generate insert statement")
			continue
		}
		_, err = retry.Exec(3, func() (sql.Result, error) {
			return r.db.ExecContext(ctx, stmt, args...)
		})
		if err != nil {
			return err
		}
	}
	return nil // TODO
}

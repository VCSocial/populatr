package dialect

import (
	"database/sql"
	"errors"

	e "github.com/vcsocial/populatr/pkg/common/err"
	"github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/info"
)

const (
	PG    = "postgres"
	MYSQL = "mysql"
)

func Connect(dbConn string, dialect string) *sql.DB {
	var db *sql.DB
	var err error
	switch dialect {
	case PG:
		db, err = sql.Open("postgres", dbConn)
	case MYSQL:
		db, err = sql.Open("mysql", dbConn) // TODO check this
	default:
		err = errors.New("unsupported dialect specified")
	}
	e.CheckPanic(err, "could not connect to database")

	return db
}

func GetRepo(dialect string) info.InfoRepo {
	switch dialect {
	case PG:
		return PostgresqlRepo{}
	default:
		logging.Global.Fatal().Msg("unsupported dialect specified")
	}
	return nil // TODO re-eval this return
}

package dialect

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/vcsocial/populatr/pkg/generator/info"
)

const (
	PG    = "postgres"
	MYSQL = "mysql"
)

func Connect(dbConn string, dialect string) (*sql.DB, error) {
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
	return db, err
}

func GetRepo(dialect string) (info.InfoRepo, error) {
	switch dialect {
	case PG:
		return PostgresqlRepo{}, nil
	default:
		errMsg := fmt.Sprintf("no repository for dialect %s", dialect)
		return nil, errors.New(errMsg)
	}
}

func GetTableRelationQuery(dialect string) (string, error) {
	switch dialect {
	case PG:
		return findAllTableRelations, nil
	default:
		errMsg := fmt.Sprintf("no query for table relations, dialect %s",
			dialect)
		return "", errors.New(errMsg)
	}
}

func GetColumnsQuery(dialect string) (string, error) {
	switch dialect {
	case PG:
		return findAllColumnsOfTable, nil
	default:
		errMsg := fmt.Sprintf("no query for getting table columns, dialect %s",
			dialect)
		return "", errors.New(errMsg)
	}
}

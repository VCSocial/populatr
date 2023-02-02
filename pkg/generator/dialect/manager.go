package dialect

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/vcsocial/populatr/pkg/common/logging"
)

const (
	PG    = "postgres"
	MYSQL = "mysql"
)

type databaseOptions struct {
	dialect  string
	username string
	password string
	host     string
	port     int
	db       string
	useSsl   bool
}

func (o *databaseOptions) Configure(dialect string, username string,
	password string, host string, port int, db string, useSsl bool) {
	o.dialect = dialect
	o.username = username
	o.password = password
	o.host = host
	o.port = port
	o.db = db
	o.useSsl = useSsl
}

type connection struct {
	driver     string
	datasource string
}

var Opts databaseOptions = databaseOptions{
	useSsl: true,
}

func getConnection(dialect string) (connection, error) {
	Opts.dialect = dialect

	driver := ""
	datasource := ""
	var err error = nil

	switch dialect {
	case PG:
		driver = PG
		datasource = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
			Opts.host, Opts.port, Opts.username, Opts.password, Opts.db)
		if !Opts.useSsl {
			datasource += " sslmode=disable"
		}
	case MYSQL:
		driver = MYSQL
		datasource = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", Opts.username, Opts.password,
			Opts.host, Opts.port, Opts.db)
		// TODO check ssl here
	default:
		driver = "undefined"
		datasource = "undefined"
		err = errors.New("unsupported dialect selected")
	}
	logging.Global.Debug().
		Str("driver", driver).
		Str("datasource", datasource).
		Msg("configuring connection")

	return connection{driver: driver, datasource: datasource}, err
}

func Connect(dialect string) (*sql.DB, error) {
	con, err := getConnection(dialect)
	if err != nil {
		logging.Global.Error().
			Err(err).
			Str("dialect", dialect).
			Msg("could not determine datasource")
	}
	db, err := sql.Open(con.driver, con.datasource)
	return db, err
}

func GetTableRelationQuery() (string, error) {
	switch Opts.dialect {
	case PG:
		return findAllTableRelationsPg, nil
	case MYSQL:
		return findAllTableRelationsMySql, nil
	default:
		errMsg := fmt.Sprintf("no query for table relations, dialect %s",
			Opts.dialect)
		return "", errors.New(errMsg)
	}
}

func GetColumnsQuery() (string, error) {
	switch Opts.dialect {
	case PG:
		return fmt.Sprintf(findAllColumnsOfTable, parameterPg), nil
	case MYSQL:
		return fmt.Sprintf(findAllColumnsOfTable, parameterMysql), nil
	default:
		errMsg := fmt.Sprintf("no query for getting table columns, dialect %s",
			Opts.dialect)
		return "", errors.New(errMsg)
	}
}

func GetInsertQueryTemplate() (string, error) {
	switch Opts.dialect {
	case PG:
		return insertQueryTemplate, nil
	case MYSQL:
		return insertQueryTemplate, nil
	default:
		errMsg := fmt.Sprintf("no query for getting table columns, dialect %s",
			Opts.dialect)
		return "", errors.New(errMsg)
	}
}

func QuoteIdentifer(identifier string) string {
	switch Opts.dialect {
	case PG:
		return pq.QuoteIdentifier(identifier)
	default:
		return identifier
	}
}

func GetPositionalParameter(position int) string {
	switch Opts.dialect {
	case PG:
		return fmt.Sprintf("$%d", position)
	case MYSQL:
		return "?"
	default:
		panic("Unhandled dialect " + Opts.dialect + " unknown parameter")
	}
}

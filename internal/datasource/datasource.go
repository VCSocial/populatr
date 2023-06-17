package datasource

import (
	"github.com/jmoiron/sqlx"
)

type Dialect string

const (
	DialectPG = "postgres"
)

type Options struct {
	Dialect  string
	Username string
	Password string
	Host     string
	Port     int
	Db       string
	UseSsl   bool
}

type OptsFunc func(*Options)

func WithDialect(dialect string) OptsFunc {
	return func(o *Options) {
		o.Dialect = dialect
	}
}

func WithUsername(username string) OptsFunc {
	return func(o *Options) {
		o.Username = username
	}
}

func WithPassword(password string) OptsFunc {
	return func(o *Options) {
		o.Password = password
	}
}

func WithHost(host string) OptsFunc {
	return func(o *Options) {
		o.Host = host
	}
}

func WithPort(port int) OptsFunc {
	return func(o *Options) {
		o.Port = port
	}
}

func WithDb(db string) OptsFunc {
	return func(o *Options) {
		o.Db = db
	}
}

func WithSsl(useSsl bool) OptsFunc {
	return func(o *Options) {
		o.UseSsl = useSsl
	}
}

func defaultOptions() Options {
	return Options{
		Host:   "localhost",
		UseSsl: false,
	}
}

type Connection struct {
	Options
}

func NewConnection(optConfigs ...OptsFunc) *Connection {
	opts := defaultOptions()
	for _, optConfig := range optConfigs {
		optConfig(&opts)
	}
	return &Connection{
		Options: opts,
	}
}

type ConnectionManager interface {
	Connect() (*sqlx.DB, error)
}

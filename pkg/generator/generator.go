package generator

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const insertQueryTemplate = "INSERT INTO %s (%s) VALUES (%s)"
const limiter int = 10000

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
}

type InfoSchemaCol struct {
	TableName              string         `field:"table_name"`
	ColumnName             string         `field:"column_name"`
	DataType               string         `field:"data_type"`
	OrdinalPosition        string         `field:"ordinal_position"`
	ColumnDefault          sql.NullString `field:"column_default"`
	CharacterMaximumLength sql.NullInt64  `field:"character_maximum_length"`
	NumericPercision       sql.NullString `field:"numeric_precision"`
	DateTimePrecision      sql.NullString `field:"datetime_precision"`
	UdtName                string         `field:"udt_name"`
	IsNullable             string         `field:"is_nullable"`
	IsGenerated            string         `field:"is_generated"`
	IsUpdatable            string         `field:"is_updatable"`
}

func (isc InfoSchemaCol) toVarchar() sql.NullString {
	size := uint(gofakeit.Number(0, int(isc.CharacterMaximumLength.Int64)))
	return sql.NullString{String: gofakeit.LetterN(size), Valid: true}
}

func (isc InfoSchemaCol) toUuid() sql.NullString {
	return sql.NullString{String: gofakeit.UUID(), Valid: true}
}

func (isc InfoSchemaCol) toTimestamp() sql.NullTime {
	start := time.Date(1, time.January, 1, 1, 1, 1, 1, time.UTC)
	end := time.Date(294275, time.December, 12, 31, 23, 59, 59, time.UTC)
	date := gofakeit.DateRange(start, end).In(time.UTC)
	return sql.NullTime{Time: date, Valid: true}
}

func (isc InfoSchemaCol) toIntX(intSize uint8) sql.NullInt64 {
	switch intSize {
	case 2:
		return sql.NullInt64{Int64: int64(gofakeit.Int16()), Valid: true}
	case 4:
		return sql.NullInt64{Int64: int64(gofakeit.Int32()), Valid: true}
	case 8:
		return sql.NullInt64{Int64: gofakeit.Int64(), Valid: true}
	default:
		return sql.NullInt64{Int64: int64(gofakeit.Int8()), Valid: true}
	}
}

type Data struct {
	SqlValue any
}

func DataFrom(isc InfoSchemaCol) (*Data, error) {
	var val any

	switch isc.UdtName {
	case "varchar":
		val = isc.toVarchar()
	case "uuid":
		val = isc.toUuid()
	case "timestamp":
		val = isc.toTimestamp()
	case "int1":
		val = isc.toIntX(1)
	case "int2":
		val = isc.toIntX(2)
	case "int4":
		val = isc.toIntX(4)
	case "int8":
		val = isc.toIntX(8)
	default:
		log.Warn().
			Str("udt_name", isc.UdtName).
			Str("table_name", isc.TableName).
			Str("column_name", isc.ColumnName)
		return nil, errors.New("Unhanlded data type " + isc.UdtName)
	}
	return &Data{SqlValue: val}, nil
}

type Record struct {
	Data map[string]Data
}

func (r *Record) Initialize() {
	r.Data = make(map[string]Data)
}

func (r *Record) Add(isc InfoSchemaCol) {
	dataPtr, err := DataFrom(isc)
	if err != nil {
		panic("TODO") //TODO
	}
	r.Data[isc.ColumnName] = *dataPtr
}

type Entity struct {
	Records   []Record
	TableName string
}

func (e *Entity) GenerateRecords(iscArr []InfoSchemaCol) {
	allRecords := []Record{}
	for i := 0; i < limiter; i++ {
		r := Record{}
		r.Initialize()
		for _, isc := range iscArr {
			r.Add(isc)
		}
		allRecords = append(allRecords, r)
	}
	e.Records = allRecords
	e.TableName = iscArr[0].TableName
}

func (e *Entity) GetInsertQuery() (string, [][]any, error) {
	if len(e.Records) < 1 {
		return "", [][]any{}, errors.New("No records assigned to entity")
	}

	j := 1
	params := []string{}
	placeholders := []string{}
	values := [][]any{}
	for i, r := range e.Records {
		row := []any{}
		for colName, val := range r.Data {
			if i == 0 {
				params = append(params, colName)
				placeholders = append(placeholders, fmt.Sprintf("$%d", j))
			}
			row = append(row, val.SqlValue)
			j++
		}
		values = append(values, row)
	}
	query := fmt.Sprintf(insertQueryTemplate, e.TableName, strings.Join(params, ","), strings.Join(placeholders, ","))
	return query, values, nil
}

type EntityRegistry struct {
	entities map[string]Entity
}

func (er *EntityRegistry) Initialize() {
	er.entities = make(map[string]Entity)
}

func (er *EntityRegistry) Register(entity Entity) {
	er.entities[entity.TableName] = entity
}

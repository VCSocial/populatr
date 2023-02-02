package mapper

import (
	"database/sql"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/info"
)

func toVarchar(length int64) sql.NullString {
	size := uint(gofakeit.Number(1, int(length)))
	return sql.NullString{String: gofakeit.LetterN(size), Valid: true}
}

func toBpchar(length int64) sql.NullString {
	sqlStr := toVarchar(length)
	sqlStrLen := len(sqlStr.String)
	if sqlStrLen < int(length) {
		remaining := int(length) - sqlStrLen
		padding := strings.Repeat(" ", remaining)
		sqlStr.String = padding + sqlStr.String
	}
	return sqlStr
}

func toBool() sql.NullBool {
	return sql.NullBool{Bool: gofakeit.Bool(), Valid: true}
}

func toBytes() sql.NullString {
	return sql.NullString{
		String: string(gofakeit.ImagePng(320, 240)),
		Valid:  false,
	}
}

func toInteger(precision int64) sql.NullInt64 {
	var randInt int64
	switch precision {
	case 16:
		randInt = int64(gofakeit.Int16())
	case 32:
		randInt = int64(gofakeit.Int32())
	case 64:
		randInt = gofakeit.Int64()
	default:
		randInt = int64(gofakeit.Int8())
	}
	return sql.NullInt64{Int64: int64(math.Abs(float64(randInt))), Valid: true}
}

func toNumeric(precision int64, scale int64) sql.NullFloat64 {
	if precision <= 0 {
		return sql.NullFloat64{Float64: 0, Valid: false}
	}

	sum := gofakeit.UintRange(1, 9)
	for i := 1; i < int(precision-scale); i++ {
		sum = (sum * 10) + gofakeit.UintRange(0, 9)
	}

	floatSum := float64(sum) + (gofakeit.Float64Range(0, 9) / 10)
	mod := math.Pow(10, float64(scale))
	floatSum = float64(int(floatSum*mod)) / mod
	return sql.NullFloat64{Float64: floatSum, Valid: true}
}

// TODO causes errors when inserting in MySQL
func toTimestamp() sql.NullTime {
	start := time.Date(1, time.January, 1, 1, 1, 1, 1, time.UTC)
	end := time.Date(294275, time.December, 12, 31, 23, 59, 59, time.UTC)
	date := gofakeit.DateRange(start, end).In(time.UTC)
	return sql.NullTime{Time: date, Valid: true}
}

func toUuid() sql.NullString {
	return sql.NullString{String: gofakeit.UUID(), Valid: true}
}

func Convert(c info.ColumnMetadata) (any, error) {
	var val any

	switch c.DataType {
	case "bool":
		val = toBool()
	case "bpchar":
		val = toBpchar(c.CharacterMaximumLength.Int64)
	case "bytea":
		val = toBytes()
	case "character varying", "varchar":
		val = toVarchar(c.CharacterMaximumLength.Int64)
	case "smallint", "int", "integer", "bigint":
		val = toInteger(c.NumericPercision.Int64)
	case "timestamp without time zone", "timestamp", "date":
		val = toTimestamp()
	case "numeric", "decimal":
		val = toNumeric(c.NumericPercision.Int64, c.NumericScale.Int64)
	case "uuid":
		val = toUuid()
	default:
		logging.Global.Warn().
			Str("data_type", c.DataType).
			Str("column_name", c.Name).
			Msg("unsupported data type")
		if c.IsNullable == "YES" {
			return sql.NullString{
				String: "",
				Valid:  false,
			}, nil
		}
		return nil, errors.New("unhanlded data type " + c.DataType)
	}
	return val, nil
}

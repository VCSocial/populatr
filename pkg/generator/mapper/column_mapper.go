package mapper

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/info"
)

func toVarchar(c info.Column, length int64) sql.NullString {
	size := uint(gofakeit.Number(1, int(length)))
	return sql.NullString{String: gofakeit.LetterN(size), Valid: true}
}

func toBpchar(c info.Column, length int64) sql.NullString {
	sqlStr := toVarchar(c, length)
	sqlStrLen := len(sqlStr.String)
	if sqlStrLen < int(length) {
		remaining := int(length) - sqlStrLen
		padding := strings.Repeat(" ", remaining)
		sqlStr.String = padding + sqlStr.String
	}
	return sqlStr
}

func toUuid() sql.NullString {
	return sql.NullString{String: gofakeit.UUID(), Valid: true}
}

func toTimestamp() sql.NullTime {
	start := time.Date(1, time.January, 1, 1, 1, 1, 1, time.UTC)
	end := time.Date(294275, time.December, 12, 31, 23, 59, 59, time.UTC)
	date := gofakeit.DateRange(start, end).In(time.UTC)
	return sql.NullTime{Time: date, Valid: true}
}

func toBool() sql.NullBool {
	return sql.NullBool{Bool: gofakeit.Bool(), Valid: true}
}

func toIntX(intSize uint8) sql.NullInt64 {
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

// TODO always overflows
func toNumericX(precision uint64) sql.NullFloat64 {
	switch precision {
	case 2:
		return sql.NullFloat64{Float64: float64(gofakeit.Int16()), Valid: true}
	case 4:
		return sql.NullFloat64{Float64: float64(gofakeit.Int16()), Valid: true}
	case 8:
		return sql.NullFloat64{Float64: float64(gofakeit.Int16()), Valid: true}
	default:
		return sql.NullFloat64{Float64: float64(gofakeit.Int16()), Valid: true}
	}
}

func toBytes() sql.NullString {
	return sql.NullString{
		String: string(gofakeit.ImagePng(320, 240)),
		Valid:  false,
	}
}

func Convert(c info.Column) (any, error) {
	var val any

	switch c.UdtName {
	case "bool":
		val = toBool()
	case "varchar":
		val = toVarchar(c, c.CharacterMaximumLength.Int64)
	case "text":
		val = toVarchar(c, 32000000)
	case "_text":
		val = toVarchar(c, 32000000)
	case "bpchar":
		val = toBpchar(c, c.CharacterMaximumLength.Int64)
	case "uuid":
		val = toUuid()
	case "timestamp":
		val = toTimestamp()
	case "date":
		val = toTimestamp()
	case "int1":
		val = toIntX(1)
	case "int2":
		val = toIntX(2)
	case "int4":
		val = toIntX(4)
	case "int8":
		val = toIntX(8)
	case "numeric":
		if c.NumericPercision.Valid {
			val = toNumericX(uint64(c.NumericPercision.Int64))
		}
	case "bytea":
		val = toBytes()
	default:
		logging.Global.Warn().
			Str("udt_name", c.UdtName).
			Str("column_name", c.Name).
			Msg("unsupported data type")
		if c.IsNullable == "YES" {
			return sql.NullString{
				String: "",
				Valid:  false,
			}, nil
		}
		return nil, errors.New("unhanlded data type " + c.UdtName)
	}
	return val, nil
}

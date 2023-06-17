package app

import (
	"context"
	"database/sql"
	"github.com/brianvoe/gofakeit/v6"
	"math"
	"populatr/internal/domain"
	"populatr/internal/logger"
	"time"

	sq "github.com/Masterminds/squirrel"
)

type GeneratorFunc[T any] func(col domain.ColumnMetadata) T

var typeGenerators = map[string]GeneratorFunc[any]{
	"character varying": func(col domain.ColumnMetadata) any {
		size := uint(gofakeit.Number(1, int(col.CharacterMaxLength.Int32)))
		return sql.NullString{String: gofakeit.LetterN(size), Valid: true}
	},
	"integer": func(col domain.ColumnMetadata) any {
		var randInt int64
		switch col.NumericPrecision.Int32 {
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
	},
	"timestamp without time zone": func(col domain.ColumnMetadata) any {
		start := time.Date(1, time.January, 1, 1, 1, 1, 1, time.UTC)
		end := time.Date(294275, time.December, 12, 31, 23, 59, 59, time.UTC)
		date := gofakeit.DateRange(start, end).In(time.UTC)
		return sql.NullTime{Time: date, Valid: true}
	},
	"numeric": func(col domain.ColumnMetadata) any {
		if col.NumericPrecision.Int32 <= 0 {
			return sql.NullFloat64{Float64: 0, Valid: false}
		}

		sum := gofakeit.UintRange(1, 9)
		for i := 1; i < int(col.NumericPrecision.Int32-col.NumericScale.Int32); i++ {
			sum = (sum * 10) + gofakeit.UintRange(0, 9)
		}

		floatSum := float64(sum) + (gofakeit.Float64Range(0, 9) / 10)
		mod := math.Pow(10, float64(col.NumericScale.Int32))
		floatSum = float64(int(floatSum*mod)) / mod
		return sql.NullFloat64{Float64: floatSum, Valid: true}
	},
}

var lgr = logger.Get()

func buildReferenceQuery(col domain.ColumnMetadata) (sq.Sqlizer, bool) {
	if col.ForeignTable.Valid && col.ForeignColumn.Valid { // TODO make constant

		q, _, _ := sq.Select("\"" + col.ForeignColumn.String + "\"").
			From("\"" + col.ForeignTable.String + "\"").
			OrderBy("random()").
			Limit(1).
			ToSql()
		return sq.Expr("(" + q + ")"), true
	}
	return nil, false
}

func (a *app) Generate() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	data, err := a.infoSchemaRepo.FindAllMetadata(ctx)
	if err != nil {
		return err
	}

	idx := 0
	tblIdx := make(map[string]int)
	genLimit := uint64(200)

	var tables []string
	var identifiers [][]string
	var values [][][]any
	for _, col := range data {
		genFunc, ok := typeGenerators[col.DataType.String]
		if !ok {
			lgr.Warn().
				Str("data_type", col.DataType.String).
				Msg("no generator found for data type")
			continue
		}

		lgr.Debug().
			Str("table_name", col.TableName.String)

		i, ok := tblIdx[col.TableName.String]
		if ok {
			identifiers[i] = append(identifiers[i], "\""+col.ColumnName.String+"\"")
		} else {
			tblIdx[col.TableName.String] = idx
			identifiers = append(identifiers, []string{"\"" + col.ColumnName.String + "\""})
			tables = append(tables, "\""+col.TableName.String+"\"")
			values = append(values, [][]any{})
			i = idx
			idx++
		}

		for j := uint64(0); j < genLimit; j++ {
			if len(values[i]) < int(genLimit) && len(values[i]) == int(j) {
				values[i] = append(values[i], []any{})
			}

			if col.ForeignTable.Valid && col.ForeignColumn.Valid {
				sub, _ := buildReferenceQuery(col)
				values[i][j] = append(values[i][j], sub)
			} else {
				values[i][j] = append(values[i][j], genFunc(col))
			}
		}
	}

	err = a.infoSchemaRepo.CreateTestData(ctx, &tables, &identifiers, &values)
	if err != nil {
		return err
	}
	return nil
}

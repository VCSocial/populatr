package retry

import (
	"errors"
	"populatr/internal/logger"
)

func Exec[T any](attempts int, fn func() (T, error)) (T, error) {
	l := logger.Get()

	var t T
	var err error
	for i := 0; i < attempts; i++ {
		t, err = fn()
		if err == nil {
			break
		}
		l.Warn().Err(err).Int("attempt", i+1).Msg("retrying")
	}

	if err != nil {
		return t, errors.Join(errors.New("exhausted retries"), err)
	}
	return t, nil
}

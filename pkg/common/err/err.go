package err

import "github.com/vcsocial/populatr/pkg/common/logging"

func CheckPanic(err error, msg string) {
	if err != nil {
		logging.Global.Fatal().Err(err).Msg(msg)
	}
}

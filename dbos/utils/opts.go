package utils

import (
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

var (
	retryLimit         = 5
	retryBackoffFactor = 2.0
	retryInterval      = 1 * time.Second
)

func GetStepOpts() []dbos.StepOption {
	opts := []dbos.StepOption{}
	opts = append(opts, dbos.WithStepMaxRetries(retryLimit))
	opts = append(opts, dbos.WithBackoffFactor(retryBackoffFactor))
	opts = append(opts, dbos.WithBaseInterval(retryInterval))
	return opts
}

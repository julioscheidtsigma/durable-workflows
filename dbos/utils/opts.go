package utils

import (
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/julioscheidtsigma/dbos/constants"
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

func GetWorkflowOpts(workflowID string) []dbos.WorkflowOption {
	opts := []dbos.WorkflowOption{}
	opts = append(opts, dbos.WithQueue(constants.QueueName))
	opts = append(opts, dbos.WithPortableWorkflow()) // marks the workflow to use JSON format for all serialized data
	opts = append(opts, dbos.WithWorkflowID(workflowID))
	return opts
}

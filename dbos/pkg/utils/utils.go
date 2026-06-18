package utils

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/julioscheidtsigma/dbos/pkg/constants"
)

var (
	retryLimit         = 5
	retryBackoffFactor = 2.0
	retryInterval      = 1 * time.Second
	// inject failures randomly based on this probability, protected by a lock to allow concurrent access and modification
	failureProbability     = 0.0 // from 0.0 to 1.0, set to 0.0 to disable failure, can be changed to test retries
	failureProbabilityLock = sync.RWMutex{}
)

func GetModuleOpts() []dbos.StepOption {
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

func SetFailureProbability(newProbability float64) error {
	if newProbability < 0.0 || newProbability > 1.0 {
		return errors.New("invalid probability value, must be between 0.0 and 1.0")
	}
	failureProbabilityLock.Lock()
	defer failureProbabilityLock.Unlock()
	failureProbability = newProbability
	return nil
}

func RandomlyFail() error {
	failureProbabilityLock.RLock()
	defer failureProbabilityLock.RUnlock()
	if failureProbability > 0.0 {
		if rand.Float64() < failureProbability {
			return fmt.Errorf("simulated random failure")
		}
	}
	return nil
}

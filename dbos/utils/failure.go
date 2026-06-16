package utils

import (
	"fmt"
	"math/rand/v2"
	"sync"
)

var (
	// inject failures randomly based on this probability, protected by a lock to allow concurrent access and modification
	failureProbability     = 0.0 // from 0.0 to 1.0, set to 0.0 to disable failure, can be changed to test retries
	failureProbabilityLock = sync.RWMutex{}
)

func ChangeFailureProbability(newProbability float64) {
	if newProbability < 0.0 || newProbability > 1.0 {
		fmt.Printf("ChangeFailureProbability: invalid probability %+v, must be between 0.0 and 1.0\n", newProbability)
		return
	}
	failureProbabilityLock.Lock()
	defer failureProbabilityLock.Unlock()

	failureProbability = newProbability
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

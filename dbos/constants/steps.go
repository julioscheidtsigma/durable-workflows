package constants

type Step int

const (
	RUN_STEP_ALL                  Step = iota // run all steps
	RUN_STEP_DATA_COLLECTION                  // run only step 1
	RUN_STEP_EVIDENCES_COLLECTION             // run only step 2
	RUN_STEP_PEP                              // run only step 3
	RUN_STEP_SANCTIONS                        // run only step 4
)

func (s Step) RunAllSteps() bool {
	return s == RUN_STEP_ALL
}

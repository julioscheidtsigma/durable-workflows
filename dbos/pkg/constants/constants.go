package constants

const (
	QueueName = "edd-queue"
)

type Module int

const (
	RUN_MODULES_ALL                  Module = iota // run all modules
	RUN_MODULES_DATA_COLLECTION                    // run only module 1
	RUN_MODULES_EVIDENCES_COLLECTION               // 2
	RUN_MODULES_PEP                                // 3
	RUN_MODULES_SANCTIONS                          // 4
	RUN_MODULES_ADVERSE_MEDIA                      // 5
	RUN_MODULES_SYNTHESIS                          // 6
)

func (m Module) RunAllModules() bool {
	return m == RUN_MODULES_ALL
}

func ParseRunModule(m int) Module {
	module := RUN_MODULES_ALL // default to run all modules
	switch m {
	case 0:
		module = RUN_MODULES_ALL
	case 1:
		module = RUN_MODULES_DATA_COLLECTION
	case 2:
		module = RUN_MODULES_EVIDENCES_COLLECTION
	case 3:
		module = RUN_MODULES_PEP
	case 4:
		module = RUN_MODULES_SANCTIONS
	case 5:
		module = RUN_MODULES_ADVERSE_MEDIA
	case 6:
		module = RUN_MODULES_SYNTHESIS
	default:
	}
	return module
}

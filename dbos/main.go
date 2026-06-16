package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
)

var (
	retryLimit         = 5
	retryBackoffFactor = 2.0
	retryInterval      = 1 * time.Second
)

var queueResultsChan = make(chan WorkflowResult, 100) // buffered channel to hold workflow results when run as queue

const (
	RUN_STEP_ALL                  int = iota // run all steps
	RUN_STEP_DATA_COLLECTION                 // run only step 1
	RUN_STEP_EVIDENCES_COLLECTION            // run only step 2
)

const (
	QUEUE = "edd-queue"
)

type WorkflowParams struct {
	URN        string `json:"urn"`
	RunAsQueue bool   `json:"runAsQueue"`
	RunStep    int    `json:"runStep"` // optional param to control which step to run, default is 0 which means run all steps
}

type WorkflowPhase1Result struct {
	OutputDataCollection      string `json:"outputDataCollection"`
	OutputEvidencesCollection string `json:"outputEvidencesCollection"`
}

type WorkflowPhase2Result struct {
	OutputPepModule       string `json:"outputPepModule"`
	OutputSanctionsModule string `json:"outputSanctionsModule"`
}

type WorkflowResult struct {
	WorkflowPhase1Result
	WorkflowPhase2Result
}

func (wr WorkflowResult) ToJSON() string {
	apiResponse, _ := json.Marshal(wr)
	return string(apiResponse)
}

type OutputStep struct {
	step   int
	output string
	err    error
}

type WorkflowItem struct {
	UUID          string    `json:"uuid"`
	Status        string    `json:"status"`
	Name          string    `json:"name"`
	Input         string    `json:"input"`
	Output        string    `json:"output"`
	Attempts      int       `json:"attempts"`
	Queue         string    `json:"queue"`
	Serialization string    `json:"serialization"`
	StartedAt     time.Time `json:"startedAt"`
	CreatedAt     time.Time `json:"createdAt"`
}

func (p WorkflowParams) GetIdempotencyKey() string {
	hash := xxhash.New()
	_, _ = hash.WriteString(p.URN)
	_, _ = hash.WriteString(strconv.FormatBool(p.RunAsQueue))
	_, _ = hash.WriteString(strconv.Itoa(int(p.RunStep)))
	return strconv.FormatUint(hash.Sum64(), 10)
}

func getStepOpts() []dbos.StepOption {
	opts := []dbos.StepOption{}
	opts = append(opts, dbos.WithStepMaxRetries(retryLimit))
	opts = append(opts, dbos.WithBackoffFactor(retryBackoffFactor))
	opts = append(opts, dbos.WithBaseInterval(retryInterval))
	return opts
}

func MainWorkflowChildPhase1(dbosCtx dbos.DBOSContext, params WorkflowParams) (WorkflowPhase1Result, error) {
	fmt.Printf("MainWorkflowChildPhase1 params: %+v\n", params)
	opts := getStepOpts()
	results := &WorkflowPhase1Result{}
	runAllSteps := params.RunStep == RUN_STEP_ALL

	// run both steps in parallel
	if runAllSteps || params.RunStep == RUN_STEP_DATA_COLLECTION {
		output, err := dbos.RunAsStep(dbosCtx, DataCollectionStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflowChildPhase1: DataCollectionStep: error %+v\n", err)
		} else {
			fmt.Printf("MainWorkflowChildPhase1: DataCollectionStep result: %+v\n", output)
			results.OutputDataCollection = output
		}
	}

	if runAllSteps || params.RunStep == RUN_STEP_EVIDENCES_COLLECTION {
		output, err := dbos.RunAsStep(dbosCtx, EvidencesCollectionStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflowChildPhase1: EvidencesCollectionStep: error %+v\n", err)
		} else {
			fmt.Printf("MainWorkflowChildPhase1: EvidencesCollectionStep result: %+v\n", output)
			results.OutputEvidencesCollection = output
		}
	}
	fmt.Printf("MainWorkflowChildPhase1: results %+v\n", results)

	if params.RunAsQueue {
		time.Sleep(15 * time.Second) // simulate some delay before starting the steps, can be removed in real implementation
	}

	return *results, nil
}

func MainWorkflowChildPhase2(dbosCtx dbos.DBOSContext, params WorkflowParams) (WorkflowPhase2Result, error) {
	fmt.Printf("MainWorkflowChildPhase2 params: %+v\n", params)
	opts := getStepOpts()
	results := &WorkflowPhase2Result{}

	// run both steps in parallel
	output, err := dbos.RunAsStep(dbosCtx, PepModuleStep, opts...)
	if err != nil {
		fmt.Printf("MainWorkflowChildPhase2: PepModuleStep: error %+v\n", err)
	} else {
		fmt.Printf("MainWorkflowChildPhase2: PepModuleStep result: %+v\n", output)
		results.OutputPepModule = output
	}

	output, err = dbos.RunAsStep(dbosCtx, SanctionsModuleStep, opts...)
	if err != nil {
		fmt.Printf("MainWorkflowChildPhase2: SanctionsModuleStep: error %+v\n", err)
	} else {
		fmt.Printf("MainWorkflowChildPhase2: SanctionsModuleStep result: %+v\n", output)
		results.OutputSanctionsModule = output
	}
	fmt.Printf("MainWorkflowChildPhase2: results %+v\n", results)

	if params.RunAsQueue {
		time.Sleep(30 * time.Second) // simulate some delay before starting the steps, can be removed in real implementation
	}

	return *results, nil
}

func MainWorkflow(dbosCtx dbos.DBOSContext, params WorkflowParams) (WorkflowResult, error) {
	// workflow id is the same as the idempotency key
	workflowID, _ := dbosCtx.GetWorkflowID()
	fmt.Printf("MainWorkflow: workflowID %+v\n", workflowID)

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)

	// run children workflows to demonstrate child workflow support
	handlePhase1, err := dbos.RunWorkflow(dbosCtx, MainWorkflowChildPhase1, params, dbos.WithQueue(QUEUE))
	if err != nil {
		return WorkflowResult{}, err
	}
	// here we are calling the results right after starting the phase,
	// to simulate dependencies between phases
	resultPhase1, err := handlePhase1.GetResult()
	if err != nil {
		return WorkflowResult{}, err
	}

	handlePhase2, err := dbos.RunWorkflow(dbosCtx, MainWorkflowChildPhase2, params, dbos.WithQueue(QUEUE))
	if err != nil {
		return WorkflowResult{}, err
	}
	resultPhase2, err := handlePhase2.GetResult()
	if err != nil {
		return WorkflowResult{}, err
	}

	results := WorkflowResult{
		WorkflowPhase1Result: resultPhase1,
		WorkflowPhase2Result: resultPhase2,
	}

	if params.RunAsQueue {
		// send results to a channel to be consumed by another goroutine
		queueResultsChan <- results
	}

	return results, nil
}

func GenericWorkflowStep(ctx context.Context, stepName string) (string, error) {
	// params := ctx.Value("params").(WorkflowParams)
	// inject random failure to test retries
	randNum := rand.IntN(2) // generates a random number between 0 and 1
	if randNum == 1 {
		return "", errors.New("simulated error in the step")
	}
	return fmt.Sprintf("%s succeeded", stepName), nil
}

func DataCollectionStep(ctx context.Context) (string, error) {
	// each step output will be stored into dbos.operation_outputs
	return GenericWorkflowStep(ctx, "DataCollectionStep")
}

func EvidencesCollectionStep(ctx context.Context) (string, error) {
	// each step output will be stored into dbos.operation_outputs
	return GenericWorkflowStep(ctx, "EvidencesCollectionStep")
}

func PepModuleStep(ctx context.Context) (string, error) {
	// each step output will be stored into dbos.operation_outputs
	return GenericWorkflowStep(ctx, "PepModuleStep")
}

func SanctionsModuleStep(ctx context.Context) (string, error) {
	// each step output will be stored into dbos.operation_outputs
	return GenericWorkflowStep(ctx, "SanctionsModuleStep")
}

func StartWorkflowHandler(dbosCtx dbos.DBOSContext, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		urn := query.Get("urn")
		runAsQueue := query.Get("runAsQueue") == "true"
		runStepStr := query.Get("runStep")
		runStep := RUN_STEP_ALL // default to run all steps
		switch runStepStr {
		case "0":
			runStep = RUN_STEP_ALL
		case "1":
			runStep = RUN_STEP_DATA_COLLECTION
		case "2":
			runStep = RUN_STEP_EVIDENCES_COLLECTION
		default:
		}

		params := WorkflowParams{
			URN:        urn,
			RunAsQueue: runAsQueue,
			RunStep:    runStep,
		}

		// the idempotency key will be used as workflow id, stored as workflow_uuid into dbos.workflow_status
		idempotencyKey := params.GetIdempotencyKey()
		fmt.Printf("StartWorkflowHandler: idempotencyKey %+v\n", idempotencyKey)

		if params.RunAsQueue {
			_, err := dbos.RunWorkflow(dbosCtx, MainWorkflow, params, dbos.WithWorkflowID(idempotencyKey), dbos.WithQueue(queue.Name))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "StartWorkflowHandler: workflow started with error %+v\n", err)
				return
			}

			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "StartWorkflowHandler: workflow triggered successfully")
		} else {
			// run a durable workflow and gather results right away
			handle, err := dbos.RunWorkflow(dbosCtx, MainWorkflow, params, dbos.WithWorkflowID(idempotencyKey))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "StartWorkflowHandler: workflow started with error %+v\n", err)
				return
			}

			output, err := handle.GetResult()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "StartWorkflowHandler: workflow finished with error %+v\n", err)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, output.ToJSON())
		}
	}
}

func ReRunWorkflowHandler(dbosCtx dbos.DBOSContext, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("uuid")
		stepStr := r.PathValue("step")
		step, _ := strconv.ParseUint(stepStr, 10, 64)
		fmt.Printf("ReRunWorkflowHandler: step %+v\n", step)

		// the idempotency key will be used as workflow id, stored as workflow_uuid into dbos.workflow_status
		forkedWorkflowID := uuid.New().String()
		fmt.Printf("ReRunWorkflowHandler: forkedWorkflowID %+v\n", forkedWorkflowID)

		handle, err := dbosCtx.ForkWorkflow(dbosCtx, dbos.ForkWorkflowInput{
			OriginalWorkflowID: workflowID,
			ForkedWorkflowID:   forkedWorkflowID,
			QueueName:          queue.Name,
			StartStep:          uint(step),
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ReRunWorkflowHandler: error re-running workflow %+v\n", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		apiResponse, _ := json.Marshal(handle)
		fmt.Fprint(w, string(apiResponse))
	}
}

func ListWorkflowsHandler(dbosCtx dbos.DBOSContext, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		workflows, err := dbos.ListWorkflows(dbosCtx, dbos.WithQueueName(QUEUE))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ListWorkflowsHandler: error listing workflows %+v\n", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		items := make([]WorkflowItem, 0)
		for _, w := range workflows {
			input := ""
			output := ""
			if w.Input != nil {
				input = w.Input.(string)
			}
			if w.Output != nil {
				output = w.Output.(string)
			}
			item := WorkflowItem{
				UUID:          w.ID,
				Status:        string(w.Status),
				Name:          w.Name,
				Input:         input,
				Output:        output,
				Attempts:      w.Attempts,
				Queue:         w.QueueName,
				Serialization: w.Serialization,
				StartedAt:     w.StartedAt,
				CreatedAt:     w.CreatedAt,
			}
			items = append(items, item)
		}

		apiResponse, _ := json.Marshal(items)
		fmt.Fprint(w, string(apiResponse))
	}
}

func CollectWorkflowResults(queueResultsChan chan WorkflowResult) {
	for result := range queueResultsChan {
		tmp := result
		go func(r WorkflowResult) {
			fmt.Printf("CollectWorkflowResults: Workflow result: %+v\n", r.ToJSON())
		}(tmp)
	}
}

func main() {
	user := "root"
	pass := "local"
	host := "localhost"
	port := 5432
	database := "argus"
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, pass, host, port, database)

	ctx := context.Background()

	conductorKey := os.Getenv("CONDUCTOR_KEY")
	// Initialize a DBOS context
	dbosCtx, err := dbos.NewDBOSContext(ctx, dbos.Config{
		DatabaseURL:     dbURL,
		AppName:         "edd",
		DatabaseSchema:  "dbos", // default
		ConductorAPIKey: conductorKey,
	})
	if err != nil {
		fmt.Printf("Error creating DBOS: %s\n", err)
	}

	// Register a workflow
	dbos.RegisterWorkflow(dbosCtx, MainWorkflow)
	dbos.RegisterWorkflow(dbosCtx, MainWorkflowChildPhase1)
	dbos.RegisterWorkflow(dbosCtx, MainWorkflowChildPhase2)

	// Create a queue
	eddQueue := dbos.NewWorkflowQueue(dbosCtx, QUEUE,
		dbos.WithWorkerConcurrency(10), // set the number of concurrent workers processing the queue
		dbos.WithRateLimiter(&dbos.RateLimiter{
			Limit:  100,
			Period: 60 * time.Second, // 100 workflows per minute
		}),
		dbos.WithPriorityEnabled(),
		dbos.WithQueueBasePollingInterval(1*time.Second),
		dbos.WithQueueMaxPollingInterval(60*time.Second),
	)

	// Launch DBOS
	errLaunch := dbos.Launch(dbosCtx)
	if errLaunch != nil {
		fmt.Printf("Error launching DBOS: %s\n", errLaunch)
	}
	// Shutdown gracefully shuts down the DBOS runtime
	defer dbos.Shutdown(dbosCtx, 30*time.Second)

	go CollectWorkflowResults(queueResultsChan)

	startWorkflowHandler := StartWorkflowHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow/start", startWorkflowHandler)

	rerunWorkflowHandler := ReRunWorkflowHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow/rerun/{uuid}/{step}", rerunWorkflowHandler)

	listWorkflowsHandler := ListWorkflowsHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow", listWorkflowsHandler)

	errListen := http.ListenAndServe(":8585", nil)
	if errListen != nil {
		fmt.Printf("Error starting server: %s\n", errListen)
	}
	close(queueResultsChan) // only reached when server exits
}

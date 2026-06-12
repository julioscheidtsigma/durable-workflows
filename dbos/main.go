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

var resultsChan = make(chan WorkflowResult, 100) // buffered channel to hold workflow results when run as queue
var eventsChan = make(chan dbos.WorkflowHandle[WorkflowResult], 100)

const (
	RUN_STEP_0 int = iota // run all steps
	RUN_STEP_1            // run only step 1
	RUN_STEP_2            // run only step 2
)

const (
	QUEUE        = "edd-queue"
	EVENT_STATUS = "status"
)

type WorkflowParams struct {
	URN        string `json:"urn"`
	RunAsQueue bool   `json:"runAsQueue"`
	RunStep    int    `json:"runStep"` // optional param to control which step to run, default is 0 which means run all steps
}

type WorkflowResult struct {
	OutputStep1 string `json:"outputStep1"`
	OutputStep2 string `json:"outputStep2"`
}

func (wr WorkflowResult) ToJSON() string {
	apiResponse, _ := json.Marshal(wr)
	return string(apiResponse)
}

type WorkflowEvent struct {
	Name string `json:"name"`
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

func MainWorkflow(dbosCtx dbos.DBOSContext, params WorkflowParams) (WorkflowResult, error) {
	// workflow id is the same as the idempotency key
	workflowID, _ := dbosCtx.GetWorkflowID()
	fmt.Printf("MainWorkflow: workflowID %+v\n", workflowID)

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)

	opts := []dbos.StepOption{}
	opts = append(opts, dbos.WithStepMaxRetries(retryLimit))
	opts = append(opts, dbos.WithBackoffFactor(retryBackoffFactor))
	opts = append(opts, dbos.WithBaseInterval(retryInterval))

	var err error
	var outputStep1 string
	var outputStep2 string
	runAllSteps := params.RunStep == RUN_STEP_0

	if runAllSteps || params.RunStep == RUN_STEP_1 {
		outputStep1, err = dbos.RunAsStep(dbosCtx, FirstWorkflowStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflow: FirstWorkflowStep: error %+v\n", err)
			return WorkflowResult{}, err
		}
		fmt.Printf("MainWorkflow: FirstWorkflowStep result: %+v\n", outputStep1)
	}

	if runAllSteps || params.RunStep == RUN_STEP_2 {
		outputStep2, err = dbos.RunAsStep(dbosCtx, SecondWorkflowStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflow: SecondWorkflowStep: error %+v\n", err)
			return WorkflowResult{}, err
		}
		fmt.Printf("MainWorkflow: SecondWorkflowStep result: %+v\n", outputStep2)
	}

	// sending events
	err = dbos.SetEvent(dbosCtx, EVENT_STATUS, WorkflowEvent{Name: "WORKFLOW_DONE"})
	if err != nil {
		return WorkflowResult{}, err
	}

	results := WorkflowResult{OutputStep1: outputStep1, OutputStep2: outputStep2}

	if params.RunAsQueue {
		// send results to a channel to be consumed by another goroutine
		resultsChan <- results
	}

	return results, nil
}

func WorkflowStep(ctx context.Context, stepName string) (string, error) {
	params := ctx.Value("params").(WorkflowParams)
	// inject random failure to test retries
	randNum := rand.IntN(2) // generates a random number between 0 and 1
	if randNum == 1 {
		return "", errors.New("simulated error in the step")
	}
	if params.RunAsQueue {
		time.Sleep(time.Duration(randNum) * time.Second)
	}
	return fmt.Sprintf("%s succeeded", stepName), nil
}

func FirstWorkflowStep(ctx context.Context) (string, error) {
	// each step output will be stored into dbos.operation_outputs
	return WorkflowStep(ctx, "FirstWorkflowStep")
}

func SecondWorkflowStep(ctx context.Context) (string, error) {
	// each step output will be stored into dbos.operation_outputs
	return WorkflowStep(ctx, "SecondWorkflowStep")
}

func StartWorkflowHandler(dbosCtx dbos.DBOSContext, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		urn := query.Get("urn")
		runAsQueue := query.Get("runAsQueue") == "true"
		runStepStr := query.Get("runStep")
		runStep := RUN_STEP_0 // default to run all steps
		switch runStepStr {
		case "0":
			runStep = RUN_STEP_0
		case "1":
			runStep = RUN_STEP_1
		case "2":
			runStep = RUN_STEP_2
		default:
		}

		params := WorkflowParams{
			URN:        urn,
			RunAsQueue: runAsQueue,
			RunStep:    runStep,
		}
		fmt.Printf("params %+v\n", params)

		// the idempotency key will be used as workflow id, stored as workflow_uuid into dbos.workflow_status
		idempotencyKey := params.GetIdempotencyKey()
		fmt.Printf("StartWorkflowHandler: idempotencyKey %+v\n", idempotencyKey)

		if params.RunAsQueue {
			handle, err := dbos.RunWorkflow(dbosCtx, MainWorkflow, params, dbos.WithWorkflowID(idempotencyKey), dbos.WithQueue(queue.Name))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "StartWorkflowHandler: workflow finished with error %+v\n", err)
				return
			}

			eventsChan <- handle

			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "StartWorkflowHandler: workflow triggered successfully")
		} else {
			// run a durable workflow and gather results right away
			handle, err := dbos.RunWorkflow(dbosCtx, MainWorkflow, params, dbos.WithWorkflowID(idempotencyKey))
			output, err := handle.GetResult()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "StartWorkflowHandler: workflow finished with error %+v\n", err)
				return
			}

			eventsChan <- handle

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, output.ToJSON())
		}
	}
}

func ReRunWorkflowHandler(dbosCtx dbos.DBOSContext, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("uuid")

		workflowSteps, err := dbosCtx.GetWorkflowSteps(dbosCtx, workflowID)
		stepFailed := uint(0)
		for _, step := range workflowSteps {
			if step.Error != nil {
				stepFailed = uint(step.StepID)
				break
			}
		}
		fmt.Printf("stepFailed %+v\n", stepFailed)

		forkedWorkflowID := uuid.New().String() // new uuid
		fmt.Printf("forkedWorkflowID %+v\n", forkedWorkflowID)
		fmt.Printf("queue.Name %+v\n", queue.Name)

		_, err = dbosCtx.ForkWorkflow(dbosCtx, dbos.ForkWorkflowInput{
			OriginalWorkflowID: workflowID,
			ForkedWorkflowID:   forkedWorkflowID,
			QueueName:          queue.Name,
			StartStep:          stepFailed,
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ReRunWorkflowHandler: error re-running workflow %+v\n", err)
			return
		}

		handle, err := dbos.ResumeWorkflow[WorkflowResult](dbosCtx, forkedWorkflowID, dbos.WithResumeQueue(queue.Name))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ReRunWorkflowHandler: error re-running workflow %+v\n", err)
			return
		}

		eventsChan <- handle

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// fmt.Fprint(w, output.ToJSON())
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
			item := WorkflowItem{
				UUID:          w.ID,
				Status:        string(w.Status),
				Name:          w.Name,
				Input:         w.Input.(string),
				Output:        w.Output.(string),
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

func CollectWorkflowResults(resultsChan chan WorkflowResult) {
	for result := range resultsChan {
		tmp := result
		go func(r WorkflowResult) {
			fmt.Printf("CollectWorkflowResults: Workflow result: %+v\n", r)
		}(tmp)
	}
}

func CollectWorkflowEvents(dbosCtx dbos.DBOSContext, eventsChan chan dbos.WorkflowHandle[WorkflowResult]) {
	for handle := range eventsChan {
		tmp := handle
		go func(h dbos.WorkflowHandle[WorkflowResult]) {
			e, err := dbos.GetEvent[WorkflowEvent](dbosCtx, h.GetWorkflowID(), EVENT_STATUS, 30*time.Second)
			if err != nil {
				return
			}
			fmt.Printf("CollectWorkflowEvents: Workflow event: %+v\n", e)
		}(tmp)
	}
}

func main() {
	user := "root"
	pass := "local"
	host := "localhost"
	port := 5432
	database := "edd"
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

	go CollectWorkflowResults(resultsChan)
	go CollectWorkflowEvents(dbosCtx, eventsChan)

	startWorkflowHandler := StartWorkflowHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow/start", startWorkflowHandler)

	rerunWorkflowHandler := ReRunWorkflowHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow/rerun/{uuid}", rerunWorkflowHandler)

	listWorkflowsHandler := ListWorkflowsHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow", listWorkflowsHandler)

	errListen := http.ListenAndServe(":8585", nil)
	if errListen != nil {
		fmt.Printf("Error starting server: %s\n", errListen)
	}
	close(resultsChan) // only reached when server exits
}

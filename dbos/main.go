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
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

var (
	retryLimit         = 5
	retryBackoffFactor = 2.0
	retryInterval      = 1 * time.Second
)

var queueResultsChan = make(chan WorkflowResult, 100) // buffered channel to hold workflow results when run as queue
var eventsChan = make(chan dbos.WorkflowHandle[WorkflowResult], 100)

const (
	RUN_STEP_ALL                  int = iota // run all steps
	RUN_STEP_DATA_COLLECTION                 // run only step 1
	RUN_STEP_EVIDENCES_COLLECTION            // run only step 2
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

func IdempotencyKeyAddSuffix(baseKey string, suffix string) string {
	hash := xxhash.New()
	_, _ = hash.WriteString(baseKey)
	_, _ = hash.WriteString(suffix)
	return strconv.FormatUint(hash.Sum64(), 10)
}

func MainWorkflowChildPhase1(dbosCtx dbos.DBOSContext, params WorkflowParams) (WorkflowPhase1Result, error) {
	fmt.Printf("MainWorkflowChildPhase1 params: %+v\n", params)

	opts := []dbos.StepOption{}
	opts = append(opts, dbos.WithStepMaxRetries(retryLimit))
	opts = append(opts, dbos.WithBackoffFactor(retryBackoffFactor))
	opts = append(opts, dbos.WithBaseInterval(retryInterval))

	type OutputStep struct {
		step   int
		output string
		err    error
	}
	runAllSteps := params.RunStep == RUN_STEP_ALL

	wg := sync.WaitGroup{}
	outputsChan := make(chan OutputStep) // channel to collect outputs from steps, will be closed after all steps are done

	// run both steps in parallel
	if runAllSteps || params.RunStep == RUN_STEP_DATA_COLLECTION {
		wg.Go(func() {
			output, err := dbos.RunAsStep(dbosCtx, DataCollectionStep, opts...)
			if err != nil {
				fmt.Printf("MainWorkflowChildPhase1: DataCollectionStep: error %+v\n", err)
				outputsChan <- OutputStep{step: RUN_STEP_DATA_COLLECTION, err: err}
			} else {
				fmt.Printf("MainWorkflowChildPhase1: DataCollectionStep result: %+v\n", output)
				outputsChan <- OutputStep{step: RUN_STEP_DATA_COLLECTION, output: output}
			}
		})
	}

	if runAllSteps || params.RunStep == RUN_STEP_EVIDENCES_COLLECTION {
		wg.Go(func() {
			output, err := dbos.RunAsStep(dbosCtx, EvidencesCollectionStep, opts...)
			if err != nil {
				fmt.Printf("MainWorkflowChildPhase1: EvidencesCollectionStep: error %+v\n", err)
				outputsChan <- OutputStep{step: RUN_STEP_EVIDENCES_COLLECTION, err: err}
			} else {
				fmt.Printf("MainWorkflowChildPhase1: EvidencesCollectionStep result: %+v\n", output)
				outputsChan <- OutputStep{step: RUN_STEP_EVIDENCES_COLLECTION, output: output}
			}
		})
	}

	go func() {
		wg.Wait()
		fmt.Printf("MainWorkflowChildPhase1: all steps done, closing outputsChan\n")
		close(outputsChan)
	}()

	// collect outputs from steps
	results := WorkflowPhase1Result{}
	for output := range outputsChan {
		if output.err != nil {
			// if any step failed, return error to trigger workflow retry
			return WorkflowPhase1Result{}, output.err
		}
		fmt.Printf("MainWorkflowChildPhase1: output.step %+v\n", output.step)
		switch output.step {
		case RUN_STEP_DATA_COLLECTION:
			results.OutputDataCollection = output.output
		case RUN_STEP_EVIDENCES_COLLECTION:
			results.OutputEvidencesCollection = output.output
		}
	}

	return results, nil
}

func MainWorkflowChildPhase2(dbosCtx dbos.DBOSContext, params WorkflowParams) (WorkflowPhase2Result, error) {
	fmt.Printf("MainWorkflowChildPhase2 params: %+v\n", params)

	opts := []dbos.StepOption{}
	opts = append(opts, dbos.WithStepMaxRetries(retryLimit))
	opts = append(opts, dbos.WithBackoffFactor(retryBackoffFactor))
	opts = append(opts, dbos.WithBaseInterval(retryInterval))

	type OutputStep struct {
		step   int
		output string
		err    error
	}

	wg := sync.WaitGroup{}
	outputsChan := make(chan OutputStep) // channel to collect outputs from steps, will be closed after all steps are done

	// run both steps in parallel
	wg.Go(func() {
		output, err := dbos.RunAsStep(dbosCtx, PepModuleStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflowChildPhase2: PepModuleStep: error %+v\n", err)
			outputsChan <- OutputStep{step: 1, err: err}
		} else {
			fmt.Printf("MainWorkflowChildPhase2: PepModuleStep result: %+v\n", output)
			outputsChan <- OutputStep{step: 1, output: output}
		}
	})

	wg.Go(func() {
		output, err := dbos.RunAsStep(dbosCtx, SanctionsModuleStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflowChildPhase2: SanctionsModuleStep: error %+v\n", err)
			outputsChan <- OutputStep{step: 2, err: err}
		} else {
			fmt.Printf("MainWorkflowChildPhase2: SanctionsModuleStep result: %+v\n", output)
			outputsChan <- OutputStep{step: 2, output: output}
		}
	})

	go func() {
		wg.Wait()
		fmt.Printf("MainWorkflowChildPhase2: all steps done, closing outputsChan\n")
		close(outputsChan)
	}()

	// collect outputs from steps
	results := WorkflowPhase2Result{}
	for output := range outputsChan {
		if output.err != nil {
			// if any step failed, return error to trigger workflow retry
			return WorkflowPhase2Result{}, output.err
		}
		fmt.Printf("MainWorkflowChildPhase2: output.step %+v\n", output.step)
		switch output.step {
		case 1:
			results.OutputPepModule = output.output
		case 2:
			results.OutputSanctionsModule = output.output
		}
	}

	return results, nil
}

func MainWorkflow(dbosCtx dbos.DBOSContext, params WorkflowParams) (WorkflowResult, error) {
	// workflow id is the same as the idempotency key
	workflowID, _ := dbosCtx.GetWorkflowID()
	fmt.Printf("MainWorkflow: workflowID %+v\n", workflowID)

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)

	// run children workflows to demonstrate child workflow support
	handlePhase1, err := dbos.RunWorkflow(dbosCtx, MainWorkflowChildPhase1, params)
	if err != nil {
		return WorkflowResult{}, err
	}
	handlePhase2, err := dbos.RunWorkflow(dbosCtx, MainWorkflowChildPhase2, params)
	if err != nil {
		return WorkflowResult{}, err
	}

	// here we are calling the results after starting all phases,
	// but this can be sequential as well if there are dependencies between phases
	resultPhase1, err := handlePhase1.GetResult()
	if err != nil {
		return WorkflowResult{}, err
	}
	// sending events
	err = dbos.SetEvent(dbosCtx, EVENT_STATUS, WorkflowEvent{Name: "PHASE_1_FINISHED"})
	if err != nil {
		return WorkflowResult{}, err
	}

	resultPhase2, err := handlePhase2.GetResult()
	if err != nil {
		return WorkflowResult{}, err
	}
	// sending events
	err = dbos.SetEvent(dbosCtx, EVENT_STATUS, WorkflowEvent{Name: "PHASE_2_FINISHED"})
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
	params := ctx.Value("params").(WorkflowParams)
	// inject random failure to test retries
	randNum := rand.IntN(2) // generates a random number between 0 and 1
	if randNum == 1 {
		return "", errors.New("simulated error in the step")
	}
	if params.RunAsQueue {
		time.Sleep(2 * time.Second)
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
			handle, err := dbos.RunWorkflow(dbosCtx, MainWorkflow, params, dbos.WithWorkflowID(idempotencyKey), dbos.WithQueue(queue.Name))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "StartWorkflowHandler: workflow started with error %+v\n", err)
				return
			}

			eventsChan <- handle

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
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ReRunWorkflowHandler: error retrieving workflow steps %+v\n", err)
			return
		}
		fmt.Printf("ReRunWorkflowHandler: workflowSteps %+v\n", workflowSteps)

		// [{StepID:0 StepName:main.MainWorkflowChildPhase1 Output:<nil> Error:<nil> ChildWorkflowID:13519425556928471554-0 StartedAt:0001-01-01 00:00:00 +0000 UTC CompletedAt:0001-01-01 00:00:00 +0000 UTC}]
		stepFailed := uint(0)
		for _, step := range workflowSteps {
			if step.Error != nil {
				stepFailed = uint(step.StepID)
				if step.StepName == "DBOS.getResult" {
					stepFailed = max(stepFailed-1, 0) // if the failure is in GetResult step, we want to rerun the workflow from the last step to make sure the workflow can complete successfully
				}
				break
			}
		}
		fmt.Printf("ReRunWorkflowHandler: stepFailed %+v\n", stepFailed)

		// the idempotency key will be used as workflow id, stored as workflow_uuid into dbos.workflow_status
		forkedWorkflowID := IdempotencyKeyAddSuffix(workflowID, fmt.Sprintf("%d", time.Now().Unix()))
		fmt.Printf("ReRunWorkflowHandler: forkedWorkflowID %+v\n", forkedWorkflowID)

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

func CollectWorkflowEvents(dbosCtx dbos.DBOSContext, eventsChan chan dbos.WorkflowHandle[WorkflowResult]) {
	for handle := range eventsChan {
		tmp := handle
		go func(h dbos.WorkflowHandle[WorkflowResult]) {
			e, err := dbos.GetEvent[WorkflowEvent](dbosCtx, h.GetWorkflowID(), EVENT_STATUS, 60*time.Second)
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
	close(queueResultsChan) // only reached when server exits
	close(eventsChan)
}

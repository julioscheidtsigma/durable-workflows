package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/julioscheidtsigma/dbos/requests"
	"github.com/julioscheidtsigma/dbos/responses"
	"github.com/julioscheidtsigma/dbos/steps"
	"github.com/julioscheidtsigma/dbos/utils"
	"github.com/julioscheidtsigma/dbos/workflows"
)

const (
	USE_WORKFLOW_CHILDREN = true
)

var (
	queueWorkerConcurrency = 10
	queueRateLimiterLimit  = 100
)

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

func WorkflowItemFromStatus(ws dbos.WorkflowStatus) WorkflowItem {
	input := ""
	output := ""
	if ws.Input != nil {
		input = ws.Input.(string)
	}
	if ws.Output != nil {
		output = ws.Output.(string)
	}
	return WorkflowItem{
		UUID:          ws.ID,
		Status:        string(ws.Status),
		Name:          ws.Name,
		Input:         input,
		Output:        output,
		Attempts:      ws.Attempts,
		Queue:         ws.QueueName,
		Serialization: ws.Serialization,
		StartedAt:     ws.StartedAt,
		CreatedAt:     ws.CreatedAt,
	}
}

func StartWorkflowHandler(dbosCtx dbos.DBOSContext, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		name := query.Get("name")
		step := steps.ParseStepFromQuery(query.Get("step"))
		fmt.Printf("StartWorkflowHandler: step %+v\n", step)

		params := requests.WorkflowParams{
			Name: name,
			Step: step,
		}
		// the idempotency key will be used as workflow id, stored as workflow_uuid into dbos.workflow_status
		// workflowID := params.IdempotencyKey()
		workflowID := uuid.New().String()
		fmt.Printf("StartWorkflowHandler: workflowID %+v\n", workflowID)

		var err error
		if USE_WORKFLOW_CHILDREN {
			_, err = dbos.RunWorkflow(dbosCtx, workflows.MainWorkflowChildren, params,
				dbos.WithWorkflowID(workflowID), dbos.WithQueue(queue.Name))
		} else {
			_, err = dbos.RunWorkflow(dbosCtx, workflows.MainWorkflow, params,
				dbos.WithWorkflowID(workflowID), dbos.WithQueue(queue.Name))
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "StartWorkflowHandler: workflow started with error %+v\n", err)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "StartWorkflowHandler: workflow triggered successfully")
	}
}

func ForkWorkflowHandler(dbosCtx dbos.DBOSContext, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("uuid")
		step, _ := strconv.ParseUint(r.PathValue("step"), 10, 64)
		fmt.Printf("ForkWorkflowHandler: step %+v\n", step)

		// the idempotency key will be used as workflow id, stored as workflow_uuid into dbos.workflow_status
		forkedWorkflowID := uuid.New().String()
		fmt.Printf("ForkWorkflowHandler: forkedWorkflowID %+v\n", forkedWorkflowID)

		handle, err := dbosCtx.ForkWorkflow(dbosCtx, dbos.ForkWorkflowInput{
			OriginalWorkflowID: workflowID,
			ForkedWorkflowID:   forkedWorkflowID,
			QueueName:          workflows.QUEUE,
			StartStep:          uint(step),
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ForkWorkflowHandler: error re-running workflow %+v\n", err)
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
		workflows, err := dbos.ListWorkflows(dbosCtx, dbos.WithQueueName(workflows.QUEUE),
			dbos.WithLoadInput(true), dbos.WithLoadOutput(true))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ListWorkflowsHandler: error listing workflows %+v\n", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		items := make([]WorkflowItem, 0)
		for _, ws := range workflows {
			items = append(items, WorkflowItemFromStatus(ws))
		}

		apiResponse, _ := json.Marshal(items)
		fmt.Fprint(w, string(apiResponse))
	}
}

func ChangeFailureProbabilityHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		probStr := query.Get("probability")
		prob, err := strconv.ParseFloat(probStr, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "ChangeFailureProbabilityHandler: invalid probability value %+v\n", probStr)
			return
		}
		utils.ChangeFailureProbability(prob)

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ChangeFailureProbabilityHandler: failure probability changed to %+v\n", prob)
	}
}

func CrashHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		os.Exit(1)
	}
}

func CollectWorkflowResults(resultsChan chan responses.WorkflowResult) {
	for result := range resultsChan {
		tmp := result
		go func(r responses.WorkflowResult) {
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

	// Register workflows
	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflow)
	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflowChildren)
	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflowPhase1)
	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflowPhase2)

	// Create a queue
	eddQueue := dbos.NewWorkflowQueue(dbosCtx, workflows.QUEUE,
		dbos.WithWorkerConcurrency(queueWorkerConcurrency),
		dbos.WithRateLimiter(&dbos.RateLimiter{
			Limit:  queueRateLimiterLimit,
			Period: 60 * time.Second,
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

	go CollectWorkflowResults(workflows.QueueResultsChan)

	startWorkflowHandler := StartWorkflowHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow/start", startWorkflowHandler)

	forkWorkflowHandler := ForkWorkflowHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow/fork/{uuid}/step/{step}", forkWorkflowHandler)

	listWorkflowsHandler := ListWorkflowsHandler(dbosCtx, eddQueue)
	http.HandleFunc("/workflow", listWorkflowsHandler)

	changeFailureProbabilityHandler := ChangeFailureProbabilityHandler()
	http.HandleFunc("/failure", changeFailureProbabilityHandler)

	crashHandler := CrashHandler()
	http.HandleFunc("/crash", crashHandler)

	errListen := http.ListenAndServe(":8585", nil)
	if errListen != nil {
		fmt.Printf("Error starting server: %s\n", errListen)
	}
	close(workflows.QueueResultsChan) // only reached when server exits
}

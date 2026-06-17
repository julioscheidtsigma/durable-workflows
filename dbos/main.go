package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/julioscheidtsigma/dbos/models"
	"github.com/julioscheidtsigma/dbos/requests"
	"github.com/julioscheidtsigma/dbos/responses"
	"github.com/julioscheidtsigma/dbos/steps"
	"github.com/julioscheidtsigma/dbos/utils"
	"github.com/julioscheidtsigma/dbos/workflows"
)

const (
	UseWorkflowChildren = false
	// queue controls
	QueueWorkerConcurrency = 10
	QueueRateLimiterLimit  = 100
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

func StartWorkflowHandler(dbosCtx dbos.DBOSContext, conn *pgx.Conn, queue dbos.WorkflowQueue) http.HandlerFunc {
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
		opts := []dbos.WorkflowOption{}
		opts = append(opts, dbos.WithQueue(queue.Name))
		opts = append(opts, dbos.WithPortableWorkflow()) // marks the workflow to use JSON format for all serialized data
		opts = append(opts, dbos.WithWorkflowID(workflowID))

		if UseWorkflowChildren {
			_, err = dbos.RunWorkflow(dbosCtx, workflows.MainWorkflowChildren, params, opts...)
		} else {
			_, err = dbos.RunWorkflow(dbosCtx, workflows.MainWorkflow, params, opts...)
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

func ForkWorkflowHandler(dbosCtx dbos.DBOSContext, conn *pgx.Conn, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		originalWorkflowID := r.PathValue("uuid")
		startStep, _ := strconv.ParseUint(r.PathValue("startStep"), 10, 64)
		fmt.Printf("ForkWorkflowHandler: startStep %+v\n", startStep)

		query := r.URL.Query()
		name := query.Get("name")
		step := steps.ParseStepFromQuery(query.Get("step"))
		params := requests.WorkflowParams{
			Name: name,
			Step: step,
		}
		fmt.Printf("ForkWorkflowHandler: params %+v\n", params)
		paramsWrapper := requests.WorkflowParamsWrapper{
			PositionalArgs: []requests.WorkflowParams{params},
			NamedArgs:      map[string]any{},
		}
		fmt.Printf("ForkWorkflowHandler: paramsWrapper %+v\n", paramsWrapper)

		originalWorkflow := models.Workflow{}
		_ = conn.QueryRow(dbosCtx, `
		SELECT
			workflow_uuid, status, name, inputs, output, error, queue_name, serialization, rate_limited, application_version
		FROM dbos.workflow_status
		WHERE workflow_uuid = $1
		LIMIT 1
		`, originalWorkflowID).Scan(
			&originalWorkflow.WorkflowUUID,
			&originalWorkflow.Status,
			&originalWorkflow.Name,
			&originalWorkflow.Inputs,
			&originalWorkflow.Output,
			&originalWorkflow.Error,
			&originalWorkflow.Queue,
			&originalWorkflow.Serialization,
			&originalWorkflow.RateLimited,
			&originalWorkflow.ApplicationVersion,
		)
		fmt.Printf("originalWorkflow: %+v\n", originalWorkflow)

		// prepare the new input for the forked workflow
		fmt.Printf("old input: %+v\n", originalWorkflow.Inputs)
		newInput := paramsWrapper.ToJSON()
		fmt.Printf("new input: %+v\n", newInput)

		insertQuery := `INSERT INTO dbos.workflow_status (
			workflow_uuid,
			status,
			name,
			application_version,
			queue_name,
			inputs,
			created_at,
			updated_at,
			recovery_attempts,
			forked_from,
			was_forked_from,
			serialization,
			rate_limited
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

		// the idempotency key will be used as workflow id, stored as workflow_uuid into dbos.workflow_status
		forkedWorkflowID := uuid.New().String()
		fmt.Printf("ForkWorkflowHandler: forkedWorkflowID %+v\n", forkedWorkflowID)

		// copy the original workflow
		_, errFork := conn.Exec(dbosCtx, insertQuery,
			forkedWorkflowID,
			"ENQUEUED", // status
			originalWorkflow.Name,
			originalWorkflow.ApplicationVersion,
			originalWorkflow.Queue,
			newInput,                       // encoded
			time.Now().UnixMilli(),         // created_at
			time.Now().UnixMilli(),         // updated_at
			0,                              // recovery_attempts
			originalWorkflowID,             // forked_from
			true,                           // was_forked_from
			originalWorkflow.Serialization, // serialization
			originalWorkflow.RateLimited,   // rate_limited
		)
		if errFork != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ForkWorkflowHandler: error forking workflow %+v\n", errFork)
			return
		}

		copyOutputsQuery := `INSERT INTO dbos.operation_outputs
			(workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
			SELECT $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
			FROM dbos.operation_outputs
			WHERE workflow_uuid = $2 AND function_id < $3`
		_, errCopy := conn.Exec(dbosCtx, copyOutputsQuery, forkedWorkflowID, originalWorkflowID, startStep)
		if errCopy != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ForkWorkflowHandler: error forking workflow %+v\n", errCopy)
			return
		}

		// handle, err := dbosCtx.ForkWorkflow(dbosCtx, dbos.ForkWorkflowInput{
		// 	OriginalWorkflowID: workflowID,
		// 	ForkedWorkflowID:   forkedWorkflowID,
		// 	QueueName:          workflows.QueueName,
		// 	StartStep:          uint(startStep),
		// })
		// if err != nil {
		// 	w.WriteHeader(http.StatusInternalServerError)
		// 	fmt.Fprintf(w, "ForkWorkflowHandler: error forking workflow %+v\n", err)
		// 	return
		// }

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// result, _ := json.Marshal(handle)
		// fmt.Fprint(w, string(result))
	}
}

func ListWorkflowsHandler(dbosCtx dbos.DBOSContext, conn *pgx.Conn, queue dbos.WorkflowQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		workflows, err := dbos.ListWorkflows(dbosCtx,
			dbos.WithQueueName(workflows.QueueName),
			dbos.WithLoadInput(true),
			dbos.WithLoadOutput(true))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "ListWorkflowsHandler: error listing workflows %+v\n", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := make([]WorkflowItem, 0)
		for _, ws := range workflows {
			response = append(response, WorkflowItemFromStatus(ws))
		}

		result, _ := json.Marshal(response)
		fmt.Fprint(w, string(result))
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

	conn, errConn := pgx.Connect(ctx, dbURL)
	if errConn != nil {
		fmt.Printf("Error connecting to database: %s\n", errConn)
	}

	conductorKey := os.Getenv("CONDUCTOR_KEY")
	// Initialize a DBOS context
	dbosCtx, errInit := dbos.NewDBOSContext(ctx, dbos.Config{
		DatabaseURL:     dbURL,
		AppName:         "edd",
		DatabaseSchema:  "dbos", // default
		ConductorAPIKey: conductorKey,
	})
	if errInit != nil {
		fmt.Printf("Error creating DBOS: %s\n", errInit)
	}

	// Register workflows
	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflow, dbos.WithWorkflowName("MainWorkflow"))
	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflowChildren, dbos.WithWorkflowName("MainWorkflowChildren"))
	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflowPhase1, dbos.WithWorkflowName("MainWorkflowPhase1"))
	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflowPhase2, dbos.WithWorkflowName("MainWorkflowPhase2"))

	// Create a queue
	rateLimiter := &dbos.RateLimiter{
		Limit:  QueueRateLimiterLimit,
		Period: 60 * time.Second,
	}
	eddQueue := dbos.NewWorkflowQueue(dbosCtx, workflows.QueueName,
		dbos.WithWorkerConcurrency(QueueWorkerConcurrency),
		dbos.WithRateLimiter(rateLimiter),
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

	startWorkflowHandler := StartWorkflowHandler(dbosCtx, conn, eddQueue)
	http.HandleFunc("/workflow/start", startWorkflowHandler)

	forkWorkflowHandler := ForkWorkflowHandler(dbosCtx, conn, eddQueue)
	http.HandleFunc("/workflow/fork/{uuid}/start/{startStep}", forkWorkflowHandler)

	listWorkflowsHandler := ListWorkflowsHandler(dbosCtx, conn, eddQueue)
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

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/julioscheidtsigma/dbos/api/requests"
	"github.com/julioscheidtsigma/dbos/api/responses"
	"github.com/julioscheidtsigma/dbos/pkg/constants"
	"github.com/julioscheidtsigma/dbos/pkg/models"
	"github.com/julioscheidtsigma/dbos/pkg/modules"
	"github.com/julioscheidtsigma/dbos/pkg/utils"
	"github.com/julioscheidtsigma/dbos/pkg/workflows"
)

const (
	// queue controls
	QueueWorkerConcurrency = 10
	QueueRateLimiterLimit  = 100
	// fields
	EnqueuedStatus = "ENQUEUED"
)

func buildErrorResponse(message string) map[string]string {
	return map[string]string{"error": message}
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

func StartWorkflowHandler(dbosCtx dbos.DBOSContext, conn *pgx.Conn, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		query := c.QueryParams()
		runModules := modules.ParseRunModule(query.Get("runModules"))
		fmt.Printf("StartWorkflowHandler: runModules %+v\n", runModules)

		params := requests.WorkflowParams{
			Name:       query.Get("name"),
			RunModules: runModules,
		}
		workflowID := uuid.New().String()
		fmt.Printf("StartWorkflowHandler: workflowID %+v\n", workflowID)

		opts := utils.GetWorkflowOpts(workflowID)
		_, err := dbos.RunWorkflow(dbosCtx, workflows.MainWorkflow, params, opts...)
		if err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error starting workflow"))
		}

		result := responses.WorkflowUUIDResult{UUID: workflowID}
		return c.JSON(http.StatusOK, result)
	}
}

func ForkWorkflowHandler(dbosCtx dbos.DBOSContext, conn *pgx.Conn, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		originalWorkflowID := c.Param("uuid")
		step, errParse := strconv.ParseUint(c.Param("step"), 10, 64)
		if errParse != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error parsing step parameter"))
		}
		fmt.Printf("ForkWorkflowHandler: step %+v\n", step)

		name := c.QueryParam("name")
		runModules := modules.ParseRunModule(c.QueryParam("runModules"))

		originalWorkflow := models.Workflow{}
		fetchQuery := `
			SELECT
				workflow_uuid, status, name, inputs, 
				output, queue_name, serialization,
				rate_limited, application_version
			FROM dbos.workflow_status
			WHERE workflow_uuid = $1
			LIMIT 1
		`
		errScan := conn.QueryRow(dbosCtx, fetchQuery, originalWorkflowID).Scan(
			&originalWorkflow.WorkflowUUID,
			&originalWorkflow.Status,
			&originalWorkflow.Name,
			&originalWorkflow.Inputs,
			&originalWorkflow.Output,
			&originalWorkflow.Queue,
			&originalWorkflow.Serialization,
			&originalWorkflow.RateLimited,
			&originalWorkflow.ApplicationVersion,
		)
		if errScan != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error fetching original workflow"))
		}
		fmt.Printf("originalWorkflow: %+v\n", originalWorkflow)

		inputs := originalWorkflow.Inputs
		fmt.Printf("old input: %+v\n", inputs)

		// prepare the new input for the forked workflow
		if name != "" {
			paramsWrapper := requests.NewWorkflowParamsWrapper(name, runModules)
			inputs = paramsWrapper.ToJSON()
			fmt.Printf("new input: %+v\n", inputs)
		}

		insertQuery := `
			INSERT INTO dbos.workflow_status (
				workflow_uuid, status, name, application_version,
				queue_name, inputs, created_at, updated_at,
				recovery_attempts, forked_from, was_forked_from,
				serialization, rate_limited
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		`

		// the idempotency key will be used as workflow id, stored as workflow_uuid into dbos.workflow_status
		forkedWorkflowID := uuid.New().String()
		fmt.Printf("ForkWorkflowHandler: forkedWorkflowID %+v\n", forkedWorkflowID)

		// copy the original workflow
		// we used the dbosCtx.ForkWorkflow(ctx, dbos.ForkWorkflowInput) method as base to copy an existing workflow
		_, errFork := conn.Exec(dbosCtx, insertQuery,
			forkedWorkflowID,
			EnqueuedStatus, // status enqueued
			originalWorkflow.Name,
			originalWorkflow.ApplicationVersion,
			originalWorkflow.Queue,
			inputs,                         // encoded
			time.Now().UnixMilli(),         // created_at
			time.Now().UnixMilli(),         // updated_at
			0,                              // recovery_attempts
			originalWorkflowID,             // forked_from
			true,                           // was_forked_from
			originalWorkflow.Serialization, // serialization
			originalWorkflow.RateLimited,   // rate_limited
		)
		if errFork != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error forking workflow"))
		}

		// SKIPPED modules will also be copied
		copyOutputsQuery := `INSERT INTO dbos.operation_outputs
			(workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
			SELECT $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
			FROM dbos.operation_outputs
			WHERE workflow_uuid = $2 AND function_id < $3`
		_, errCopy := conn.Exec(dbosCtx, copyOutputsQuery, forkedWorkflowID, originalWorkflowID, step)
		if errCopy != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error forking workflow"))
		}

		result := responses.WorkflowUUIDResult{UUID: forkedWorkflowID}
		return c.JSON(http.StatusOK, result)
	}
}

func ListWorkflowsHandler(dbosCtx dbos.DBOSContext, conn *pgx.Conn, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		workflows, err := dbos.ListWorkflows(dbosCtx,
			dbos.WithQueueName(constants.QueueName),
			dbos.WithLoadInput(true),
			dbos.WithLoadOutput(true))
		if err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error listing workflows"))
		}

		result := make([]WorkflowItem, 0)
		for _, ws := range workflows {
			result = append(result, WorkflowItemFromStatus(ws))
		}
		return c.JSON(http.StatusOK, result)
	}
}

func ChangeFailureProbabilityHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		probStr := c.QueryParam("probability")
		prob, err := strconv.ParseFloat(probStr, 64)
		if err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("invalid probability value, must be between 0.0 and 1.0"))
		}
		err = utils.SetFailureProbability(prob)
		if err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse(err.Error()))
		}
		return c.JSON(http.StatusOK, map[string]string{"message": "failure probability updated"})
	}
}

func CrashHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		os.Exit(1)
		return nil
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
	// Create a queue
	rateLimiter := &dbos.RateLimiter{
		Limit:  QueueRateLimiterLimit,
		Period: 60 * time.Second,
	}
	eddQueue := dbos.NewWorkflowQueue(dbosCtx, constants.QueueName,
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

	e := echo.New()
	e.POST("/workflow/start", StartWorkflowHandler(dbosCtx, conn, eddQueue))
	e.POST("/workflow/fork/:uuid/start/:step", ForkWorkflowHandler(dbosCtx, conn, eddQueue))
	e.GET("/workflow", ListWorkflowsHandler(dbosCtx, conn, eddQueue))
	e.POST("/failure", ChangeFailureProbabilityHandler())
	e.POST("/crash", CrashHandler())

	errListen := e.Start(":8585")
	if errListen != nil {
		fmt.Printf("Error starting server: %s\n", errListen)
	}
	close(workflows.QueueResultsChan) // only reached when server exits
}

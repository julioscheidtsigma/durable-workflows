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
	"github.com/labstack/echo/v4/middleware"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/julioscheidtsigma/dbos/api/requests"
	"github.com/julioscheidtsigma/dbos/api/responses"
	"github.com/julioscheidtsigma/dbos/db"
	"github.com/julioscheidtsigma/dbos/pkg/constants"
	"github.com/julioscheidtsigma/dbos/pkg/models"
	"github.com/julioscheidtsigma/dbos/pkg/modules"
	"github.com/julioscheidtsigma/dbos/pkg/utils"
	"github.com/julioscheidtsigma/dbos/pkg/workflows"
)

const (
	// queue controls
	QueueWorkerConcurrency = 10 // this will make the worker pick up to 10 workflows concurrently
	QueueRateLimiterLimit  = 100
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
		var req requests.WorkflowRequest
		if err := c.Bind(&req); err != nil {
			fmt.Printf("StartWorkflowHandler: error binding request: %v\n", err)
			return c.JSON(http.StatusBadRequest, buildErrorResponse("invalid payload"))
		}
		if err := req.Validate(); err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse(err.Error()))
		}
		if req.Name == nil || req.RunModules == nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("name and runModules are required"))
		}

		params := requests.WorkflowParams{
			Name:       *req.Name,
			RunModules: constants.ParseRunModule(*req.RunModules),
		}
		workflowID := uuid.New().String()
		opts := utils.BuildWorkflowOpts(workflowID)
		_, err := dbos.RunWorkflow(dbosCtx, workflows.MainWorkflow, params, opts...)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, buildErrorResponse("error starting workflow"))
		}

		result := responses.WorkflowUUIDResult{UUID: workflowID}
		return c.JSON(http.StatusOK, result)
	}
}

func ForkWorkflowHandler(dbosCtx dbos.DBOSContext, conn *pgx.Conn, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		originalWorkflowID := c.Param("uuid")
		forkStep, errParse := strconv.ParseInt(c.Param("forkStep"), 10, 64)
		if errParse != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error parsing fork step parameter"))
		}
		fmt.Printf("ForkWorkflowHandler: originalWorkflowID %+v\n", originalWorkflowID)
		fmt.Printf("ForkWorkflowHandler: forkStep %+v\n", forkStep)

		originalWorkflow, errScan := db.GetWorkflow(dbosCtx, conn, originalWorkflowID)
		if errScan != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error fetching original workflow"))
		}

		var req requests.WorkflowRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("invalid payload"))
		}
		if err := req.Validate(); err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse(err.Error()))
		}

		inputs := originalWorkflow.Inputs
		fmt.Printf("ForkWorkflowHandler: old input: %+v\n", inputs)
		// prepare the new input for the forked workflow
		if req.Name != nil && req.RunModules != nil {
			paramsWrapper := requests.NewWorkflowParamsWrapper(*req.Name, constants.ParseRunModule(*req.RunModules))
			inputs = paramsWrapper.ToJSON()
			fmt.Printf("ForkWorkflowHandler: new input: %+v\n", inputs)
		}

		forkedWorkflowID := uuid.New().String()
		fmt.Printf("ForkWorkflowHandler: forkedWorkflowID %+v\n", forkedWorkflowID)

		// using dbosCtx.ForkWorkflow(ctx, dbos.ForkWorkflowInput) method as base to copy an existing workflow
		errInsert := db.InsertWorkflow(dbosCtx, conn, forkedWorkflowID, inputs, originalWorkflow)
		if errInsert != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error forking workflow"))
		}

		// SKIPPED modules will also be copied
		errCopy := db.CopyWorkflowOutputs(dbosCtx, conn, forkedWorkflowID, originalWorkflowID, forkStep)
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

func GetWorkflowExecutionGraphHandler(dbosCtx dbos.DBOSContext, conn *pgx.Conn, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		workflowID := c.Param("uuid")
		steps, err := db.GetWorkflowStepsWithLevels(dbosCtx, conn, workflowID)
		if err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error fetching workflow steps"))
		}

		stepsByLevelMap := make(map[int][]models.WorkflowNode)
		for _, step := range steps {
			name := step.StepName
			if name == workflows.StartLevelName {
				name = fmt.Sprintf("%s%d", workflows.LevelPrefix, step.GlobalLevel)
			}
			skipped := step.Status != nil && *step.Status == modules.SkippedModule
			failed := step.Status != nil && *step.Status == modules.FailedModule
			stepsByLevelMap[step.GlobalLevel] = append(stepsByLevelMap[step.GlobalLevel], models.WorkflowNode{
				Node:     name,
				Children: []string{},
				Skipped:  skipped,
				Failed:   failed,
			})
		}

		for level, stepsByLevel := range stepsByLevelMap {
			// their children will be the next level if available
			children, exists := stepsByLevelMap[level+1]
			childrenWithLevel := make([]string, 0)
			if exists {
				for _, child := range children {
					childrenWithLevel = append(childrenWithLevel, child.Node)
				}
			}
			for idx, step := range stepsByLevel {
				if exists {
					newStep := step
					newStep.Children = childrenWithLevel
					stepsByLevelMap[level][idx] = newStep
				}
			}
		}

		return c.JSON(http.StatusOK, stepsByLevelMap)
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
	user := "root" // TODO: get from env variable or config
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
	dbosCtx, errInit := dbos.NewDBOSContext(ctx, dbos.Config{
		DatabaseURL:     dbURL,
		AppName:         "edd",
		DatabaseSchema:  "dbos", // default
		ConductorAPIKey: conductorKey,
	})
	if errInit != nil {
		fmt.Printf("Error creating DBOS: %s\n", errInit)
	}

	dbos.RegisterWorkflow(dbosCtx, workflows.MainWorkflow, dbos.WithWorkflowName("MainWorkflow"))

	rateLimiter := &dbos.RateLimiter{
		Limit:  QueueRateLimiterLimit,
		Period: 60 * time.Second,
	}
	eddQueue := dbos.NewWorkflowQueue(dbosCtx, constants.QueueName,
		dbos.WithWorkerConcurrency(QueueWorkerConcurrency),
		dbos.WithRateLimiter(rateLimiter),
		dbos.WithPriorityEnabled(),
		dbos.WithQueueBasePollingInterval(1*time.Second),
		dbos.WithQueueMaxPollingInterval(120*time.Second),
	)

	errLaunch := dbos.Launch(dbosCtx)
	if errLaunch != nil {
		fmt.Printf("Error launching DBOS: %s\n", errLaunch)
	}
	defer dbos.Shutdown(dbosCtx, 30*time.Second)

	go CollectWorkflowResults(workflows.QueueResultsChan)

	e := echo.New()
	e.POST("/workflow", StartWorkflowHandler(dbosCtx, conn, eddQueue))
	e.POST("/workflow/:uuid/fork/:forkStep", ForkWorkflowHandler(dbosCtx, conn, eddQueue))
	e.GET("/workflow", ListWorkflowsHandler(dbosCtx, conn, eddQueue))
	e.GET("/workflow/:uuid/graph", GetWorkflowExecutionGraphHandler(dbosCtx, conn, eddQueue))
	e.POST("/failure/injection", ChangeFailureProbabilityHandler())
	e.POST("/crash", CrashHandler())

	// allow Access-Control-Allow-Origin
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept},
	}))

	errListen := e.Start(":8585")
	if errListen != nil {
		fmt.Printf("Error starting server: %s\n", errListen)
	}
	close(workflows.QueueResultsChan) // only reached when server exits
}

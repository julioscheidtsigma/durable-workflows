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

func StartWorkflowHandler(dbosCtx dbos.DBOSContext, database *db.Database, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req requests.WorkflowRequest
		if err := c.Bind(&req); err != nil {
			fmt.Printf("StartWorkflowHandler: error binding request: %+v\n", err)
			return c.JSON(http.StatusBadRequest, buildErrorResponse("invalid payload"))
		}
		if err := req.Validate(); err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse(err.Error()))
		}
		if req.Name == nil || req.RunModules == nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("name and runModules are required"))
		}

		params := requests.WorkflowRequestParams{
			Name:       *req.Name,
			RunModules: constants.ParseRunModule(*req.RunModules),
		}
		workflowID := uuid.New().String()
		fmt.Printf("StartWorkflowHandler: workflowID %+v\n", workflowID)

		opts := utils.BuildWorkflowOpts(workflowID)
		_, err := dbos.RunWorkflow(dbosCtx, workflows.MainWorkflow, params, opts...)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, buildErrorResponse("error starting workflow"))
		}

		result := responses.WorkflowUUIDResult{WorkflowUUID: workflowID}
		return c.JSON(http.StatusOK, result)
	}
}

func ForkWorkflowHandler(dbosCtx dbos.DBOSContext, database *db.Database, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		var err error

		originalWorkflowID := c.Param("workflowUUID")
		fmt.Printf("ForkWorkflowHandler: originalWorkflowID %+v\n", originalWorkflowID)

		// params to choose whether to fork the workflow FROM a specific step or ONLY a specific step
		var startStep *int64 = nil
		if startStepStr := c.QueryParam("startStep"); startStepStr != "" {
			startStepParse, err := strconv.ParseInt(startStepStr, 10, 64)
			if err != nil {
				return c.JSON(http.StatusBadRequest, buildErrorResponse("error parsing start step parameter"))
			}
			startStep = &startStepParse
			fmt.Printf("ForkWorkflowHandler: startStep %+v\n", *startStep)
		}
		var onlyStep *int64 = nil
		if onlyStepStr := c.QueryParam("onlyStep"); onlyStepStr != "" {
			onlyStepParse, err := strconv.ParseInt(onlyStepStr, 10, 64)
			if err != nil {
				return c.JSON(http.StatusBadRequest, buildErrorResponse("error parsing only step parameter"))
			}
			onlyStep = &onlyStepParse
			fmt.Printf("ForkWorkflowHandler: onlyStep %+v\n", *onlyStep)
		}

		var req requests.WorkflowRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("invalid payload"))
		}
		if err := req.Validate(); err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse(err.Error()))
		}

		ctx := context.Background()

		fmt.Printf("ForkWorkflowHandler: starting transaction\n")
		tx, err := database.BeginTransaction(ctx)
		if err != nil {
			fmt.Printf("ForkWorkflowHandler: error starting database transaction: %+v\n", err)
			return c.JSON(http.StatusInternalServerError, buildErrorResponse("error starting database transaction"))
		}

		defer func() {
			if err != nil {
				fmt.Printf("ForkWorkflowHandler: rolling back transaction, due to error %+v\n", err)
				_ = database.RollbackTransaction(tx, ctx)
			}
		}()

		originalWorkflow, err := database.GetWorkflow(dbosCtx, originalWorkflowID)
		if err != nil {
			fmt.Printf("ForkWorkflowHandler: error fetching original workflow: %+v\n", err)
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error fetching original workflow"))
		}

		// check if the workflow is in ERROR or SUCCESS status, if not, return an error
		if originalWorkflow.Status != db.WorkflowStatusError && originalWorkflow.Status != db.WorkflowStatusSucess {
			fmt.Printf("ForkWorkflowHandler: rolling back transaction\n")
			_ = database.RollbackTransaction(tx, ctx)
			return c.JSON(http.StatusBadRequest, buildErrorResponse("can only fork workflows that are in ERROR or SUCCESS status"))
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
		err = database.InsertWorkflow(dbosCtx, forkedWorkflowID, inputs, originalWorkflow)
		if err != nil {
			fmt.Printf("ForkWorkflowHandler: error inserting workflow: %+v\n", err)
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error forking workflow"))
		}

		// SKIPPED modules will also be copied
		err = database.CopyWorkflowOutputs(dbosCtx, forkedWorkflowID, originalWorkflowID, startStep, onlyStep)
		if err != nil {
			fmt.Printf("ForkWorkflowHandler: error copying workflow: %+v\n", err)
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error forking workflow"))
		}

		fmt.Printf("ForkWorkflowHandler: committing transaction\n")
		err = database.CommitTransaction(tx, ctx)
		if err != nil {
			fmt.Printf("ForkWorkflowHandler: error commiting database transaction: %+v\n", err)
			return c.JSON(http.StatusInternalServerError, buildErrorResponse("error committing database transaction"))
		}

		result := responses.WorkflowUUIDResult{WorkflowUUID: forkedWorkflowID}
		return c.JSON(http.StatusOK, result)
	}
}

func ListWorkflowsHandler(dbosCtx dbos.DBOSContext, database *db.Database, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		workflows, err := dbos.ListWorkflows(dbosCtx,
			dbos.WithQueueName(constants.QueueName),
			dbos.WithLoadInput(true),
			dbos.WithLoadOutput(true))
		if err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error listing workflows"))
		}

		result := make([]models.Workflow, 0)
		for _, ws := range workflows {
			result = append(result, models.WorkflowFromStatus(ws))
		}
		return c.JSON(http.StatusOK, result)
	}
}

func GetWorkflowExecutionGraphHandler(dbosCtx dbos.DBOSContext, database *db.Database, queue dbos.WorkflowQueue) echo.HandlerFunc {
	return func(c echo.Context) error {
		workflowID := c.Param("workflowUUID")
		steps, err := database.GetWorkflowStepsWithLevels(dbosCtx, workflowID)
		if err != nil {
			return c.JSON(http.StatusBadRequest, buildErrorResponse("error fetching workflow steps"))
		}

		stepsByGlobalLevelMap := make(map[int][]models.WorkflowNode)
		for _, step := range steps {
			name := step.StepName
			if name == workflows.StartLevelName {
				name = fmt.Sprintf("%s%d", workflows.LevelPrefix, step.GlobalLevel)
			}
			skipped := step.Status != nil && *step.Status == modules.SkippedModule
			failed := step.Status != nil && *step.Status == modules.FailedModule
			var output *responses.ModuleResult
			if step.Output != nil {
				err := json.Unmarshal([]byte(*step.Output), &output)
				if err != nil {
					return c.JSON(http.StatusInternalServerError, buildErrorResponse("error unmarshaling step output"))
				}
			}
			stepsByGlobalLevelMap[step.GlobalLevel] = append(stepsByGlobalLevelMap[step.GlobalLevel], models.WorkflowNode{
				Node:        name,
				Children:    []string{},
				Skipped:     skipped,
				Failed:      failed,
				Output:      output,
				GlobalLevel: step.GlobalLevel,
				LocalLevel:  step.LocalLevel,
			})
		}

		for level, stepsByLevel := range stepsByGlobalLevelMap {
			// each level children will be the next level if available
			children, exists := stepsByGlobalLevelMap[level+1]
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
					stepsByGlobalLevelMap[level][idx] = newStep
				}
			}
		}

		return c.JSON(http.StatusOK, stepsByGlobalLevelMap)
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
	dbName := "argus"
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, pass, host, port, dbName)

	ctx := context.Background()
	conn, errConn := pgx.Connect(ctx, dbURL)
	if errConn != nil {
		fmt.Printf("Error connecting to database: %s\n", errConn)
	}
	database := db.NewDatabase(conn)

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
	e.POST("/workflow", StartWorkflowHandler(dbosCtx, database, eddQueue))
	e.POST("/workflow/:workflowUUID/fork", ForkWorkflowHandler(dbosCtx, database, eddQueue))
	e.GET("/workflow", ListWorkflowsHandler(dbosCtx, database, eddQueue))
	e.GET("/workflow/:workflowUUID/graph", GetWorkflowExecutionGraphHandler(dbosCtx, database, eddQueue))
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

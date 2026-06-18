package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/julioscheidtsigma/dbos/pkg/models"
)

const (
	// fields
	EnqueuedStatus = "ENQUEUED"
)

func InsertWorkflow(ctx context.Context, conn *pgx.Conn, workflowID, inputs string, originalWorkflow models.Workflow) error {
	query := `
		INSERT INTO dbos.workflow_status (
			workflow_uuid, status, name, application_version,
			queue_name, inputs, created_at, updated_at,
			recovery_attempts, forked_from, was_forked_from,
			serialization, rate_limited
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`
	nowUnix := time.Now().UnixMilli()
	_, errInsert := conn.Exec(ctx, query,
		workflowID,
		EnqueuedStatus, // status enqueued
		originalWorkflow.Name,
		originalWorkflow.ApplicationVersion,
		originalWorkflow.Queue,
		inputs,                         // encoded
		nowUnix,                        // created_at
		nowUnix,                        // updated_at
		0,                              // recovery_attempts
		originalWorkflow.WorkflowUUID,  // forked_from
		true,                           // was_forked_from
		originalWorkflow.Serialization, // serialization
		originalWorkflow.RateLimited,   // rate_limited
	)
	return errInsert
}

func CopyWorkflowOutputs(ctx context.Context, conn *pgx.Conn, workflowID, originalWorkflowID string, step int64) error {
	query := `
		INSERT INTO dbos.operation_outputs
		(workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
		SELECT $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
		FROM dbos.operation_outputs
		WHERE workflow_uuid = $2 AND function_id < $3
	`
	_, errUpdate := conn.Exec(ctx, query, workflowID, originalWorkflowID, step)
	return errUpdate
}

func GetWorkflow(ctx context.Context, conn *pgx.Conn, workflowID string) (models.Workflow, error) {
	workflow := models.Workflow{}
	query := `
		SELECT
			workflow_uuid, status, name, inputs, 
			output, queue_name, serialization,
			rate_limited, application_version
		FROM dbos.workflow_status
		WHERE workflow_uuid = $1
		LIMIT 1
	`
	errScan := conn.QueryRow(ctx, query, workflowID).Scan(
		&workflow.WorkflowUUID,
		&workflow.Status,
		&workflow.Name,
		&workflow.Inputs,
		&workflow.Output,
		&workflow.Queue,
		&workflow.Serialization,
		&workflow.RateLimited,
		&workflow.ApplicationVersion,
	)
	return workflow, errScan
}

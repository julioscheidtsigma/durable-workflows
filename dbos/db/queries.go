package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/julioscheidtsigma/dbos/pkg/models"
)

const (
	WorkflowStatusEnqueued = "ENQUEUED"
	WorkflowStatusError    = "ERROR"
	WorkflowStatusSucess   = "SUCCESS"
)

type Database struct {
	conn *pgx.Conn
}

func NewDatabase(conn *pgx.Conn) *Database {
	return &Database{conn: conn}
}

func (db *Database) BeginTransaction(ctx context.Context) (pgx.Tx, error) {
	tx, err := db.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	db.conn = tx.Conn()
	return tx, nil
}

func (db *Database) CommitTransaction(tx pgx.Tx, ctx context.Context) error {
	return tx.Commit(ctx)
}

func (db *Database) RollbackTransaction(tx pgx.Tx, ctx context.Context) error {
	return tx.Rollback(ctx)
}

func (db *Database) InsertWorkflow(ctx context.Context, workflowID, inputs string, originalWorkflow models.Workflow) error {
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
	_, err := db.conn.Exec(ctx, query,
		workflowID,
		WorkflowStatusEnqueued,
		originalWorkflow.Name,
		originalWorkflow.ApplicationVersion, // application_version
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
	return err
}

func (db *Database) CopyWorkflowOutputs(ctx context.Context, workflowID, originalWorkflowID string, step int64) error {
	query := `
		INSERT INTO dbos.operation_outputs
		(workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
		SELECT $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
		FROM dbos.operation_outputs
		WHERE workflow_uuid = $2 AND function_id < $3
	`
	_, err := db.conn.Exec(ctx, query, workflowID, originalWorkflowID, step)
	return err
}

func (db *Database) GetWorkflow(ctx context.Context, workflowID string) (models.Workflow, error) {
	query := `
		SELECT
			workflow_uuid, status, name, inputs, 
			output, queue_name, serialization,
			rate_limited, application_version
		FROM dbos.workflow_status
		WHERE workflow_uuid = $1
		LIMIT 1
	`
	var workflow = models.Workflow{}
	row := db.conn.QueryRow(ctx, query, workflowID)
	err := row.Scan(
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
	if workflow.WorkflowUUID == "" {
		return models.Workflow{}, pgx.ErrNoRows
	}
	return workflow, err
}

func (db *Database) GetWorkflowStepsWithLevels(ctx context.Context, workflowID string) ([]models.WorkflowStepWithLevel, error) {
	query := `
		SELECT
			oo.workflow_uuid,
			oo.function_name,
			(string_to_array(replace(oo.function_name, 'Level:', ''), ':'))[2]::text as step_name,
			(string_to_array(replace(oo.function_name, 'Level:', ''), ':'))[1]::int AS global_level,
			oo.function_id AS local_level,
			oo.output::json ->> 'status' AS status,
			CAST(CASE
					WHEN oo.serialization = 'portable_json' THEN oo.output
					ELSE decode(oo.output, 'base64')::text
			end AS json) AS output,
			CAST(CASE
					WHEN ws.serialization = 'portable_json' THEN ws.inputs
					ELSE decode(ws.inputs, 'base64')::text
			end AS json) AS inputs,
			to_timestamp(oo.started_at_epoch_ms/1000.0) at time zone 'UTC' AS started_at,
			to_timestamp(oo.completed_at_epoch_ms/1000.0) at time zone 'UTC' AS completed_at
		FROM dbos.operation_outputs oo
		JOIN dbos.workflow_status ws on ws.workflow_uuid = oo.workflow_uuid
		WHERE oo.workflow_uuid = $1
		ORDER BY global_level ASC, local_level ASC
		LIMIT 100
	`
	var steps = []models.WorkflowStepWithLevel{}
	rows, err := db.conn.Query(ctx, query, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var step models.WorkflowStepWithLevel
		err := rows.Scan(
			&step.WorkflowUUID,
			&step.FunctionName,
			&step.StepName,
			&step.GlobalLevel,
			&step.LocalLevel,
			&step.Status,
			&step.Output,
			&step.Inputs,
			&step.StartedAt,
			&step.CompletedAt,
		)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, err
}

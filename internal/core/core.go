package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	uuid "github.com/gofrs/uuid/v5"
	"github.com/kalbhor/tasqueue/v2"
	"github.com/vmihailenco/msgpack"
	"github.com/zerodha/dungbeetle/v2/internal/engine"
	"github.com/zerodha/dungbeetle/v2/models"
)

var reJobID = regexp.MustCompile(`^[a-zA-Z0-9_:-]+$`)

// Opt represents core options.
type Opt struct {
	DefaultQueue            string
	DefaultGroupConcurrency int
	DefaultJobTTL           time.Duration

	// DSNs for connecting to the broker backend and the broker state backend.
	Broker  tasqueue.Broker
	Results tasqueue.Results
}

type Core struct {
	opt Opt

	// DuckDB 2.0 Engine
	engine *engine.DuckDBEngine

	// Named SQL query tasks.
	tasks Tasks

	// Distributed queue system.
	q *tasqueue.Server

	// Job states for cancellation.
	jobCtx map[string]context.CancelFunc
	mu     sync.RWMutex

	// Prometheus/VictoriaMetrics metrics.
	metrics *metrics.Set

	lo *slog.Logger
}

// New returns a new instance of Core.
func New(o Opt, eng *engine.DuckDBEngine, lo *slog.Logger) *Core {
	return &Core{
		opt:     o,
		engine:  eng,
		tasks:   make(Tasks),
		jobCtx:  make(map[string]context.CancelFunc),
		mu:      sync.RWMutex{},
		metrics: metrics.NewSet(),
		lo:      lo,
	}
}

// Metrics returns the core's Prometheus/VictoriaMetrics metrics set.
func (co *Core) Metrics() *metrics.Set {
	return co.metrics
}

// Start is a blocking function that spawns the queue workers and starts processing jobs.
// This should be invoked last after all other initializations are done.
func (co *Core) Start(ctx context.Context, workerName string, concurrency int) error {
	// Initialize the distributed queue system.
	qs, err := co.initQueue()
	if err != nil {
		return err
	}
	co.q = qs
	qs.Start(ctx)

	return nil
}

// GetTasks returns the registered tasks map.
func (co *Core) GetTasks() Tasks {
	return co.tasks
}

// NewJob creates a new job out of a given task and registers it.
func (co *Core) NewJob(j models.JobReq, taskName string) (models.JobResp, error) {
	sig, err := co.makeJob(j, taskName)
	if err != nil {
		return models.JobResp{}, err
	}

	// Create the job.
	jobUUID, err := co.q.Enqueue(context.Background(), sig)
	if err != nil {
		return models.JobResp{}, err
	}
	co.metrics.GetOrCreateCounter(metricName(metricJobsEnqueued, "task_name", taskName)).Inc()

	return models.JobResp{
		JobID:    jobUUID,
		TaskName: sig.Task,
		Queue:    sig.Opts.Queue,
		Retries:  int(sig.Opts.MaxRetries),
		ETA:      &sig.Opts.ETA,
	}, nil
}

func (co *Core) NewJobGroup(req models.GroupReq) (models.GroupResp, error) {
	// Create job signatures for all the jobs in the group.
	sigs := make([]tasqueue.Job, 0, len(req.Jobs))
	for _, j := range req.Jobs {
		sig, err := co.makeJob(j, j.TaskName)
		if err != nil {
			return models.GroupResp{}, err
		}

		sigs = append(sigs, sig)
	}

	// Create the group and queue it.
	g, err := tasqueue.NewGroup(sigs, tasqueue.GroupOpts{
		ID: req.GroupID,
	})
	if err != nil {
		return models.GroupResp{}, err
	}

	gID, err := co.q.EnqueueGroup(context.Background(), g)
	if err != nil {
		return models.GroupResp{}, err
	}

	jobs := make([]models.JobResp, len(sigs))
	for i, r := range sigs {
		co.metrics.GetOrCreateCounter(metricName(metricJobsEnqueued, "task_name", r.Task)).Inc()

		jobs[i] = models.JobResp{
			JobID:    r.Opts.ID,
			TaskName: r.Task,
			Queue:    r.Opts.Queue,
			Retries:  int(r.Opts.MaxRetries),
			ETA:      &r.Opts.ETA,
		}
	}

	return models.GroupResp{
		GroupID: gID,
		Jobs:    jobs,
	}, nil
}

// GetPendingJobs returns jobs pending execution.
func (co *Core) GetPendingJobs(queue string) ([]tasqueue.JobMessage, error) {
	out, err := co.q.GetPending(context.Background(), queue)
	if err != nil {
		co.lo.Error("error fetching pending tasks", "error", err)
		return nil, err
	}
	jLen := len(out)
	for i := 0; i < jLen/2; i++ {
		out[i], out[jLen-i-1] = out[jLen-i-1], out[i]
	}

	return out, nil
}

// GetJobStatus returns the status of a job.
func (co *Core) GetJobStatus(jobID string) (models.JobStatusResp, error) {
	ctx := context.Background()
	out, err := co.q.GetJob(ctx, jobID)
	if err == tasqueue.ErrNotFound {
		return models.JobStatusResp{}, fmt.Errorf("job not found")
	} else if err != nil {
		co.lo.Error("error fetching job status", "error", err)
		return models.JobStatusResp{}, err
	}

	// Try fetching the job's result and ignore in case
	// the result is not found (job could be processing)
	res, err := co.q.GetResult(ctx, jobID)
	if err != nil && !errors.Is(err, tasqueue.ErrNotFound) {
		co.lo.Error("error fetching job results", "error", err)
		return models.JobStatusResp{}, err
	}

	var payload models.JobResultPayload
	if len(res) > 0 {
		if err := json.Unmarshal(res, &payload); err != nil {
			// legacy fallback if plain row count int was stored
			if count, err := strconv.ParseInt(string(res), 10, 64); err == nil {
				payload.Count = count
			}
		}
	}

	state, err := getState(out.Status)
	if err != nil {
		co.lo.Error("error fetching job status mapping", "error", err)
		return models.JobStatusResp{}, err
	}

	return models.JobStatusResp{
		JobID:     out.ID,
		State:     state,
		Count:     payload.Count,
		Table:     payload.Table,
		Catalog:   payload.Catalog,
		Files:     payload.Files,
		SizeBytes: payload.SizeBytes,
		Error:     out.PrevErr,
	}, nil
}

// GetJobGroupStatus returns the status of a job group including statuses of individual jobs in it.
func (co *Core) GetJobGroupStatus(groupID string) (models.GroupStatusResp, error) {
	ctx := context.Background()

	groupMsg, err := co.q.GetGroup(ctx, groupID)
	if err == tasqueue.ErrNotFound {
		return models.GroupStatusResp{}, fmt.Errorf("group not found")
	} else if err != nil {
		co.lo.Error("error fetching group status", "error", err)
		return models.GroupStatusResp{}, err
	}

	var jobs []models.JobStatusResp
	for uuid := range groupMsg.JobStatus {
		jobResp, err := co.GetJobStatus(uuid)
		if err != nil {
			co.lo.Error("error fetching group job status", "error", err)
			return models.GroupStatusResp{}, err
		}

		jobs = append(jobs, jobResp)
	}

	state, err := getState(groupMsg.Status)
	if err != nil {
		co.lo.Error("error fetching group status mapping", "error", err)
		return models.GroupStatusResp{}, err
	}

	return models.GroupStatusResp{
		GroupID: groupID,
		State:   state,
		Jobs:    jobs,
	}, nil
}

// CancelJob cancels a given pending/processing job.
func (co *Core) CancelJob(jobID string, purge bool) error {
	s, err := co.GetJobStatus(jobID)
	if err != nil {
		co.lo.Error("error fetching job", "error", err)
		return errors.New("error fetching job")
	}

	// If the job is already complete, no go.
	if !purge && (s.State == "SUCCESS" || s.State == "FAILURE") {
		return errors.New("can't cancel completed job")
	}

	// Stop the job if it's running.
	co.mu.RLock()
	cancel, ok := co.jobCtx[jobID]
	co.mu.RUnlock()
	if ok {
		cancel()
	}

	// Delete the job.
	if err := co.q.DeleteJob(context.Background(), jobID); err != nil {
		co.lo.Error("error deleting job", "error", err)
		return fmt.Errorf("error deleting job: %v", err)
	}

	return nil
}

// CancelJobGroup cancels a pending group job by individually cancelling all pending jobs in it.
func (co *Core) CancelJobGroup(groupID string, purge bool) error {
	res, err := co.GetJobGroupStatus(groupID)
	if err != nil {
		co.lo.Error("error fetching group status", "error", err)
		return errors.New("error fetching group status")
	}

	if !purge {
		s, err := co.q.GetGroup(context.Background(), groupID)
		if err == tasqueue.ErrNotFound {
			return fmt.Errorf("group not found")
		} else if err != nil {
			co.lo.Error("error fetching group status", "error", err)
			return err
		}

		if s.Status == tasqueue.StatusDone || s.Status == tasqueue.StatusFailed {
			return errors.New("can't delete group as it's already complete")
		}
	}

	for _, j := range res.Jobs {
		co.mu.RLock()
		cancel, ok := co.jobCtx[j.JobID]
		co.mu.RUnlock()
		if ok {
			cancel()
		}

		if err := co.q.DeleteJob(context.Background(), j.JobID); err != nil {
			co.lo.Error("error deleting job", "error", err)
			return fmt.Errorf("error deleting job: %v", err)
		}
	}

	return nil
}

func (co *Core) makeJob(j models.JobReq, taskName string) (tasqueue.Job, error) {
	if j.JobID != "" && !reJobID.MatchString(j.JobID) {
		return tasqueue.Job{}, errors.New("invalid characters in the `job_id`")
	}

	task, ok := co.tasks[taskName]
	if !ok {
		return tasqueue.Job{}, fmt.Errorf("unrecognized task: %s", taskName)
	}

	msg, err := co.q.GetJob(context.Background(), j.JobID)
	if err != nil && !errors.Is(err, tasqueue.ErrNotFound) {
		return tasqueue.Job{}, fmt.Errorf("error fetching job status (id='%s')", j.JobID)
	}

	if msg.Status == tasqueue.StatusProcessing || msg.Status == tasqueue.StatusRetrying {
		return tasqueue.Job{}, fmt.Errorf("job '%s' is already running", j.JobID)
	}

	if j.JobID == "" {
		uid, err := uuid.NewV4()
		if err != nil {
			return tasqueue.Job{}, fmt.Errorf("error generating uuid: %v", err)
		}

		j.JobID = fmt.Sprintf("job_%v", uid)
	}

	ttl := co.opt.DefaultJobTTL
	if j.TTL > 0 {
		ttl = time.Duration(j.TTL) * time.Second
	}

	var eta time.Time
	if j.ETA != "" {
		e, err := time.Parse("2006-01-02 15:04:05", j.ETA)
		if err != nil {
			return tasqueue.Job{}, fmt.Errorf("error parsing ETA: %v", err)
		}

		eta = e
	}

	if j.Queue == "" {
		j.Queue = task.Queue
	}

	var args = make([]interface{}, len(j.Args))
	for i := range j.Args {
		args[i] = j.Args[i]
	}

	readyAt := time.Now()
	if eta.After(readyAt) {
		readyAt = eta
	}

	b, err := msgpack.Marshal(taskMeta{
		Args:    args,
		DB:      j.DB,
		TTL:     int(ttl),
		ReadyAt: readyAt,
	})
	if err != nil {
		return tasqueue.Job{}, err
	}

	return tasqueue.NewJob(taskName, b, tasqueue.JobOpts{
		ID:         j.JobID,
		Queue:      j.Queue,
		MaxRetries: uint32(j.Retries),
		ETA:        eta,
	})
}

type taskMeta struct {
	Args    []interface{} `json:"args"`
	DB      string        `json:"db"`
	TTL     int           `json:"ttl"`
	ReadyAt time.Time     `json:"ready_at"`
}

func (co *Core) initQueue() (*tasqueue.Server, error) {
	qs, err := tasqueue.NewServer(tasqueue.ServerOpts{
		Broker:  co.opt.Broker,
		Results: co.opt.Results,
		Logger:  co.lo.Handler(),
	})
	if err != nil {
		return nil, err
	}

	for name := range co.tasks {
		query := co.tasks[name]
		co.registerQueueMetrics(query.Queue, query.Conc)

		var (
			running   = co.metrics.GetOrCreateGauge(metricName(metricJobsRunning, "task_name", name), nil)
			succeeded = co.metrics.GetOrCreateCounter(metricName(metricJobAttempts, "task_name", name, "outcome", "success"))
			errored   = co.metrics.GetOrCreateCounter(metricName(metricJobAttempts, "task_name", name, "outcome", "error"))
			canceled  = co.metrics.GetOrCreateCounter(metricName(metricJobAttempts, "task_name", name, "outcome", "canceled"))
			execTime  = co.metrics.GetOrCreatePrometheusHistogramExt(metricName(metricJobExecDuration, "task_name", name), durationBuckets)
		)

		err := qs.RegisterTask(string(name), func(b []byte, jctx tasqueue.JobCtx) (err error) {
			running.Inc()
			defer func() {
				running.Dec()
				switch {
				case err == nil:
					succeeded.Inc()
				case errors.Is(err, errJobCanceled), errors.Is(err, tasqueue.ErrNotFound):
					canceled.Inc()
				default:
					errored.Inc()
				}
			}()

			if _, err = qs.GetJob(context.Background(), jctx.Meta.ID); err != nil {
				return err
			}

			var args taskMeta
			if err = msgpack.Unmarshal(b, &args); err != nil {
				return fmt.Errorf("could not unmarshal args : %w", err)
			}

			if jctx.Meta.Retried == 0 && !args.ReadyAt.IsZero() {
				wait := max(time.Since(args.ReadyAt), 0)
				co.metrics.GetOrCreatePrometheusHistogramExt(
					metricName(metricJobQueueWait, "task_name", name, "queue", jctx.Meta.Queue),
					durationBuckets,
				).Update(wait.Seconds())
			}

			start := time.Now()
			payload, err := co.execJob(jctx.Meta.ID, name, args.DB, args.Args, query)
			execTime.UpdateDuration(start)
			if err != nil {
				return fmt.Errorf("could not execute job : %w", err)
			}

			bPayload, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("could not marshal result payload: %w", err)
			}

			return jctx.Save(bPayload)
		}, tasqueue.TaskOpts{
			Concurrency: uint32(query.Conc),
			Queue:       query.Queue,
		})
		if err != nil {
			return nil, err
		}
	}

	return qs, nil
}

var errJobCanceled = errors.New("the job was canceled")

// execJob executes an SQL statement job directly via DuckDBEngine to DuckLake.
func (co *Core) execJob(jobID, taskName, dbName string, args []interface{}, task Task) (models.JobResultPayload, error) {
	if _, err := co.GetJobStatus(jobID); err != nil {
		return models.JobResultPayload{}, errJobCanceled
	}

	// Determine source database:
	// 1. Explicit DB in request
	// 2. Tagged DB in task (random if multiple)
	// 3. Fallback to any configured source
	sourceDB := dbName
	if sourceDB == "" && len(task.DBs) > 0 {
		sourceDB = task.DBs[rand.Intn(len(task.DBs))]
	}
	if sourceDB == "" {
		for s := range co.engine.GetSources() {
			sourceDB = s
			break
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()

		co.mu.Lock()
		delete(co.jobCtx, jobID)
		co.mu.Unlock()
	}()

	co.mu.Lock()
	co.jobCtx[jobID] = cancel
	co.mu.Unlock()

	payload, err := co.engine.ExecuteJob(ctx, jobID, sourceDB, task.Raw, args...)
	if err != nil {
		if ctx.Err() != nil {
			return models.JobResultPayload{}, errJobCanceled
		}
		return models.JobResultPayload{}, fmt.Errorf("task execution failed: %s: %v", task.Name, err)
	}

	return payload, nil
}

const (
	StatusPending = "PENDING"
	StatusStarted = "STARTED"
	StatusFailure = "FAILURE"
	StatusSuccess = "SUCCESS"
	StatusRetry   = "RETRY"
)

func getState(st string) (string, error) {
	switch st {
	case tasqueue.StatusStarted:
		return StatusPending, nil
	case tasqueue.StatusProcessing:
		return StatusStarted, nil
	case tasqueue.StatusFailed:
		return StatusFailure, nil
	case tasqueue.StatusDone:
		return StatusSuccess, nil
	case tasqueue.StatusRetrying:
		return StatusRetry, nil
	}

	return "", fmt.Errorf("invalid status not found in mapping")
}

package models

import (
	"time"
)

// JobReq represents a job request.
type JobReq struct {
	TaskName string   `json:"task"`
	JobID    string   `json:"job_id"`
	Queue    string   `json:"queue"`
	ETA      string   `json:"eta"`
	Retries  int      `json:"retries"`
	TTL      int      `json:"ttl"`
	Args     []string `json:"args"`
	DB       string   `json:"db"`

	ttlDuration time.Duration
}

// JobResp is the response sent to a job request.
type JobResp struct {
	JobID    string     `json:"job_id"`
	TaskName string     `json:"task"`
	Queue    string     `json:"queue"`
	ETA      *time.Time `json:"eta"`
	Retries  int        `json:"retries"`
}

// JobStatusResp represents the response of a single job.
type JobStatusResp struct {
	JobID     string   `json:"job_id"`
	State     string   `json:"state"`
	Count     int64    `json:"count"`
	Table     string   `json:"table,omitempty"`
	Catalog   string   `json:"catalog,omitempty"`
	Files     []string `json:"files,omitempty"`
	SizeBytes int64    `json:"size_bytes,omitempty"`
	Error     string   `json:"error,omitempty"`
}

// JobResultPayload represents the serialized payload saved in the queue backend.
type JobResultPayload struct {
	Count     int64    `json:"count"`
	Table     string   `json:"table"`
	Catalog   string   `json:"catalog"`
	Files     []string `json:"files"`
	SizeBytes int64    `json:"size_bytes"`
}

// GroupReq represents a Jobrequest group.
type GroupReq struct {
	GroupID string   `json:"group_id"`
	Jobs    []JobReq `json:"jobs"`
}

// GroupResp is the response sent to a group job request.
type GroupResp struct {
	GroupID string    `json:"group_id"`
	Jobs    []JobResp `json:"jobs"`
}

// GroupStatusResp represents the status of a group job.
type GroupStatusResp struct {
	GroupID string          `json:"group_id"`
	State   string          `json:"state"`
	Jobs    []JobStatusResp `json:"jobs"`
}

// HTTPResp represents a container for generic outgoing HTTP
// responses.
type HTTPResp struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data"`
}

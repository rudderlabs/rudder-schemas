package cluster

import (
	"path"
	"slices"
	"time"

	"github.com/samber/lo"
)

type PartitionMigrationStatus string

const (
	PartitionMigrationStatusNew                PartitionMigrationStatus = "new"                 // initial state
	PartitionMigrationStatusReloadingGW        PartitionMigrationStatus = "reloading-gw"        // reloading gateway nodes
	PartitionMigrationStatusReloadingSrcRouter PartitionMigrationStatus = "reloading-srcrouter" // reloading source routers
	PartitionMigrationStatusMigrating          PartitionMigrationStatus = "migrating"           // migrating partitions
	PartitionMigrationStatusCompleted          PartitionMigrationStatus = "completed"           // migration completed
)

// PartitionMigration represents the overall migration process for a set of partitions.
type PartitionMigration struct {
	ID             string                         `json:"id"`             // unique identifier for the migration
	Status         PartitionMigrationStatus       `json:"status"`         // current status of the migration
	PreviousStatus PartitionMigrationStatus       `json:"previousStatus"` // previous status of the migration
	Jobs           []*PartitionMigrationJobHeader `json:"jobs"`           // list of migration jobs
	StartTime      time.Time                      `json:"startTime"`      // time when the migration was started

	AckKeyPrefix string `json:"ackKeyPrefix"` // the key prefix to use for acknowledging the migration initialization
}

// PartitionMigrationJobHeader contains the basic information about a partition migration job.
type PartitionMigrationJobHeader struct {
	JobID      string   `json:"jobId"`      // unique identifier for the migration job
	SourceNode int      `json:"sourceNode"` // Index of the source node
	TargetNode int      `json:"targetNode"` // Index of the target node
	Partitions []string `json:"partitions"` // List of partition IDs being migrated
}

// Clone clones the PartitionMigrationJobHeader.
func (pmj *PartitionMigrationJobHeader) Clone() *PartitionMigrationJobHeader {
	return &PartitionMigrationJobHeader{
		JobID:      pmj.JobID,
		SourceNode: pmj.SourceNode,
		TargetNode: pmj.TargetNode,
		Partitions: slices.Clone(pmj.Partitions),
	}
}

// SourceNodes returns a list of unique source node indexes involved in the migration.
func (pm *PartitionMigration) SourceNodes() []int {
	return lo.Keys(lo.SliceToMap(pm.Jobs,
		func(job *PartitionMigrationJobHeader) (int, struct{}) {
			return job.SourceNode, struct{}{}
		}),
	)
}

// TargetNodes returns a list of unique target node indexes involved in the migration.
func (pm *PartitionMigration) TargetNodes() []int {
	return lo.Keys(lo.SliceToMap(pm.Jobs,
		func(job *PartitionMigrationJobHeader) (int, struct{}) {
			return job.TargetNode, struct{}{}
		}),
	)
}

// Ack creates an acknowledgment for a node involved in the migration.
func (pm *PartitionMigration) Ack(nodeIndex int, nodeName string) *PartitionMigrationAck {
	return &PartitionMigrationAck{
		NodeIndex: nodeIndex,
		NodeName:  nodeName,
	}
}

// AckKey generates the acknowledgment key for a given node name.
func (pm *PartitionMigration) AckKey(nodeName string) string {
	return path.Join(pm.AckKeyPrefix, nodeName)
}

// Clone creates a deep copy of the PartitionMigration.
func (pm *PartitionMigration) Clone() *PartitionMigration {
	return &PartitionMigration{
		ID:             pm.ID,
		Status:         pm.Status,
		PreviousStatus: pm.PreviousStatus,
		Jobs: lo.Map(pm.Jobs, func(job *PartitionMigrationJobHeader, _ int) *PartitionMigrationJobHeader {
			return job.Clone()
		}),
		StartTime:    pm.StartTime,
		AckKeyPrefix: pm.AckKeyPrefix,
	}
}

// PartitionMigrationAck represents an acknowledgment from a node regarding the migration.
type PartitionMigrationAck struct {
	NodeIndex int    `json:"nodeIndex"` // Index of the node acknowledging
	NodeName  string `json:"nodeName"`  // Name of the node acknowledging
}

// ReloadGatewayCommand represents a command to reload the gateway nodes during migration.
type ReloadGatewayCommand struct {
	Nodes []int `json:"nodes"` // list of gateway node indices to reload

	AckKeyPrefix string `json:"ackKeyPrefix"` // the key prefix to use for acknowledging the reload
}

// Ack creates an acknowledgment for a gateway node after reloading.
func (rg *ReloadGatewayCommand) Ack(nodeIndex int, nodeName string) *ReloadGatewayAck {
	return &ReloadGatewayAck{
		NodeIndex: nodeIndex,
		NodeName:  nodeName,
	}
}

// AckKey generates the acknowledgment key for a given node name.
func (rg *ReloadGatewayCommand) AckKey(nodeName string) string {
	return path.Join(rg.AckKeyPrefix, nodeName)
}

// ReloadGatewayAck represents an acknowledgment from a gateway node after reloading.
type ReloadGatewayAck struct {
	NodeIndex int    `json:"nodeIndex"` // Index of the node acknowledging
	NodeName  string `json:"nodeName"`  // Name of the node acknowledging
}

// ReloadSrcRouterCommand represents a command to reload the source routers during migration.
type ReloadSrcRouterCommand struct {
	AckKeyPrefix string `json:"ackKeyPrefix"` // the key prefix to use for acknowledging the reload
}

// Ack creates an acknowledgment for a source router node after reloading.
func (rr *ReloadSrcRouterCommand) Ack(nodeName string) *ReloadSrcRouterAck {
	return &ReloadSrcRouterAck{
		NodeName: nodeName,
	}
}

// AckKey generates the acknowledgment key for a given node name.
func (rr *ReloadSrcRouterCommand) AckKey(nodeName string) string {
	return path.Join(rr.AckKeyPrefix, nodeName)
}

// ReloadSrcRouterAck represents an acknowledgment from the srcrouter after reloading.
type ReloadSrcRouterAck struct {
	NodeName string `json:"nodeName"` // Name of the node acknowledging
}

type PartitionMigrationJobStatus string

const (
	PartitionMigrationJobStatusNew       PartitionMigrationJobStatus = "new"       // initial state
	PartitionMigrationJobStatusMoved     PartitionMigrationJobStatus = "moved"     // partitions have been moved
	PartitionMigrationJobStatusCompleted PartitionMigrationJobStatus = "completed" // migration job completed
)

// PartitionMigrationJob represents a specific migration job for a set of partitions.
type PartitionMigrationJob struct {
	PartitionMigrationJobHeader
	MigrationID string                      `json:"migrationId"`
	Status      PartitionMigrationJobStatus `json:"status"`
	StartTime   time.Time                   `json:"startTime"` // time when the migration job was started
}

// PartitionMigrationInfo represents the information about an ongoing partition migration, including job details.
type PartitionMigrationInfo struct {
	ID             string                   `json:"id"`             // unique identifier for the migration
	Status         PartitionMigrationStatus `json:"status"`         // current status of the migration
	PreviousStatus PartitionMigrationStatus `json:"previousStatus"` // previous status of the migration
	Jobs           []*PartitionMigrationJob `json:"jobs"`           // list of migration jobs
	StartTime      time.Time                `json:"startTime"`      // time when the migration was started

	AckKeyPrefix string `json:"ackKeyPrefix"` // the key prefix to use for acknowledging the migration initialization
}

// Clone creates a deep copy of the PartitionMigrationInfo.
func (pmi *PartitionMigrationInfo) Clone() *PartitionMigrationInfo {
	return &PartitionMigrationInfo{
		ID:             pmi.ID,
		Status:         pmi.Status,
		PreviousStatus: pmi.PreviousStatus,
		Jobs: lo.Map(pmi.Jobs, func(job *PartitionMigrationJob, _ int) *PartitionMigrationJob {
			return job.Clone()
		}),
		StartTime:    pmi.StartTime,
		AckKeyPrefix: pmi.AckKeyPrefix,
	}
}

// Clone creates a deep copy of the PartitionMigrationJob.
func (pmj *PartitionMigrationJob) Clone() *PartitionMigrationJob {
	return &PartitionMigrationJob{
		PartitionMigrationJobHeader: *pmj.PartitionMigrationJobHeader.Clone(),
		MigrationID:                 pmj.MigrationID,
		Status:                      pmj.Status,
		StartTime:                   pmj.StartTime,
	}
}

func (pmi *PartitionMigrationInfo) FromPartitionMigration(pm PartitionMigration, jobStatusMap map[string]PartitionMigrationJobStatus) {
	if pmi == nil {
		return
	}
	pmi.ID = pm.ID
	pmi.Status = pm.Status
	pmi.PreviousStatus = pm.PreviousStatus
	pmi.Jobs = lo.Map(pm.Jobs, func(job *PartitionMigrationJobHeader, _ int) *PartitionMigrationJob {
		return &PartitionMigrationJob{
			PartitionMigrationJobHeader: *job.Clone(),
			MigrationID:                 pm.ID,
			Status:                      jobStatusMap[job.JobID],
		}
	})
	pmi.AckKeyPrefix = pm.AckKeyPrefix
	pmi.StartTime = pm.StartTime
}

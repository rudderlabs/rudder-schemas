package cluster

import (
	"path"

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
	ID     string                         `json:"id"`     // unique identifier for the migration
	Status PartitionMigrationStatus       `json:"status"` // current status of the migration
	Jobs   []*PartitionMigrationJobHeader `json:"jobs"`   // list of migration jobs

	AckKeyPrefix string `json:"ackKeyPrefix"` // the key prefix to use for acknowledging the migration initialization
}

// PartitionMigrationJobHeader contains the basic information about a partition migration job.
type PartitionMigrationJobHeader struct {
	JobID      string   `json:"jobId"`      // unique identifier for the migration job
	SourceNode int      `json:"sourceNode"` // Index of the source node
	TargetNode int      `json:"targetNode"` // Index of the target node
	Partitions []string `json:"partitions"` // List of partition IDs being migrated
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
}

package cluster

import "github.com/samber/lo"

type PartitionMigrationStatus string

const (
	PartitionMigrationStatusNew             PartitionMigrationStatus = "new"              // initial state
	PartitionMigrationStatusReloadingGW     PartitionMigrationStatus = "reloading-gw"     // reloading gateway nodes
	PartitionMigrationStatusReloadingRouter PartitionMigrationStatus = "reloading-router" // reloading source routers
	PartitionMigrationStatusMigrating       PartitionMigrationStatus = "migrating"        // migrating partitions
	PartitionMigrationStatusCompleted       PartitionMigrationStatus = "completed"        // migration completed
)

// PartitionMigration represents the overall migration process for a set of partitions.
type PartitionMigration struct {
	ID     string                         `json:"id"`     // unique identifier for the migration
	Status PartitionMigrationStatus       `json:"status"` // current status of the migration
	Jobs   []*PartitionMigrationJobHeader `json:"jobs"`   // list of migration jobs

	AckKey string `json:"ackKey"` // the key to use for acknowledging the migration initialization
}

// PartitionMigrationJobHeader contains the basic information about a partition migration job.
type PartitionMigrationJobHeader struct {
	JobID      string   `json:"jobId"`      // unique identifier for the migration job
	SourceNode int      `json:"sourceNode"` // Index of the source node
	TargetNode int      `json:"targetNode"` // Index of the target node
	Partitions []string `json:"partitions"` // List of partition IDs being migrated
}

// SourceNodes returns a list of unique source node IDs involved in the migration.
func (pm *PartitionMigration) SourceNodes() []int {
	return lo.Keys(lo.SliceToMap(pm.Jobs,
		func(job *PartitionMigrationJobHeader) (int, struct{}) {
			return job.SourceNode, struct{}{}
		}),
	)
}

// TargetNodes returns a list of unique target node IDs involved in the migration.
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

// PartitionMigrationAck represents an acknowledgment from a node regarding the migration.
type PartitionMigrationAck struct {
	NodeIndex int    `json:"nodeIndex"` // Index of the node acknowledging
	NodeName  string `json:"nodeName"`  // Name of the node acknowledging
}

// ReloadGatewayCommand represents a command to reload the gateway nodes during migration.
type ReloadGatewayCommand struct {
	Nodes []int `json:"nodes"` // list of gateway node indices to reload

	AckKey string `json:"ackKey"` // the key to use for acknowledging the reload
}

func (rg *ReloadGatewayCommand) Ack(nodeIndex int, nodeName string) *ReloadGatewayAck {
	return &ReloadGatewayAck{
		NodeIndex: nodeIndex,
		NodeName:  nodeName,
	}
}

// ReloadGatewayAck represents an acknowledgment from a gateway node after reloading.
type ReloadGatewayAck struct {
	NodeIndex int    `json:"nodeIndex"` // Index of the node acknowledging
	NodeName  string `json:"nodeName"`  // Name of the node acknowledging
}

// ReloadRouterCommand represents a command to reload the router after migration.
type ReloadRouterCommand struct {
	AckKey string `json:"ackKey"` // the key to use for acknowledging the reload
}

func (rr *ReloadRouterCommand) Ack(nodeName string) *ReloadRouterAck {
	return &ReloadRouterAck{
		NodeName: nodeName,
	}
}

// ReloadRouterAck represents an acknowledgment from the router after reloading.
type ReloadRouterAck struct {
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

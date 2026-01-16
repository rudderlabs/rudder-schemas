package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-schemas/go/cluster"
)

func TestMigrationTypes(t *testing.T) {
	t.Run("PartitionMigration", func(t *testing.T) {
		m := &cluster.PartitionMigration{
			ID:     "id",
			Status: cluster.PartitionMigrationStatusNew,
			Jobs: []*cluster.PartitionMigrationJobHeader{
				{
					JobID:      "job-1",
					SourceNode: 0,
					TargetNode: 1,
					Partitions: []string{"ws1-0", "ws1-1"},
				},
				{
					JobID:      "job-2",
					SourceNode: 0,
					TargetNode: 2,
					Partitions: []string{"ws1-2", "ws1-3"},
				},
			},
			AckKeyPrefix: "ack",
		}

		t.Run("marshal unmarshal", func(t *testing.T) {
			data, err := jsonrs.Marshal(m)
			require.NoError(t, err)

			var unmarshaled cluster.PartitionMigration
			err = jsonrs.Unmarshal(data, &unmarshaled)
			require.NoError(t, err)
			require.Equal(t, m, &unmarshaled)
		})

		t.Run("SourceNodes", func(t *testing.T) {
			sourceNodes := m.SourceNodes()
			require.ElementsMatch(t, sourceNodes, []int{0})
		})

		t.Run("TargetNodes", func(t *testing.T) {
			targetNodes := m.TargetNodes()
			require.ElementsMatch(t, targetNodes, []int{1, 2})
		})

		t.Run("Ack", func(t *testing.T) {
			ack := m.Ack(0, "node-0")
			expectedAck := &cluster.PartitionMigrationAck{
				NodeIndex: 0,
				NodeName:  "node-0",
			}
			require.Equal(t, expectedAck, ack)
		})

		t.Run("AckKey", func(t *testing.T) {
			ackKey := m.AckKey("node-0")
			require.Equal(t, "ack/node-0", ackKey)
		})
	})

	t.Run("ReloadGatewayCommand", func(t *testing.T) {
		cmd := &cluster.ReloadGatewayCommand{
			Nodes:        []int{0, 1, 2},
			AckKeyPrefix: "ack",
		}

		t.Run("marshal unmarshal", func(t *testing.T) {
			data, err := jsonrs.Marshal(cmd)
			require.NoError(t, err)

			var unmarshaled cluster.ReloadGatewayCommand
			err = jsonrs.Unmarshal(data, &unmarshaled)
			require.NoError(t, err)
			require.Equal(t, cmd, &unmarshaled)
		})

		t.Run("Ack", func(t *testing.T) {
			ack := cmd.Ack(0, "node-0")
			expectedAck := &cluster.ReloadGatewayAck{
				NodeIndex: 0,
				NodeName:  "node-0",
			}
			require.Equal(t, expectedAck, ack)
		})

		t.Run("AckKey", func(t *testing.T) {
			ackKey := cmd.AckKey("node-0")
			require.Equal(t, "ack/node-0", ackKey)
		})
	})

	t.Run("ReloadSrcRouterCommand", func(t *testing.T) {
		cmd := &cluster.ReloadSrcRouterCommand{
			AckKeyPrefix: "ack",
		}

		t.Run("marshal unmarshal", func(t *testing.T) {
			data, err := jsonrs.Marshal(cmd)
			require.NoError(t, err)

			var unmarshaled cluster.ReloadSrcRouterCommand
			err = jsonrs.Unmarshal(data, &unmarshaled)
			require.NoError(t, err)
			require.Equal(t, cmd, &unmarshaled)
		})

		t.Run("Ack", func(t *testing.T) {
			ack := cmd.Ack("node-0")
			expectedAck := &cluster.ReloadSrcRouterAck{
				NodeName: "node-0",
			}
			require.Equal(t, expectedAck, ack)
		})

		t.Run("AckKey", func(t *testing.T) {
			ackKey := cmd.AckKey("node-0")
			require.Equal(t, "ack/node-0", ackKey)
		})
	})

	t.Run("PartitionMigrationJob", func(t *testing.T) {
		job := &cluster.PartitionMigrationJob{
			PartitionMigrationJobHeader: cluster.PartitionMigrationJobHeader{
				JobID:      "job-1",
				SourceNode: 0,
				TargetNode: 1,
				Partitions: []string{"ws1-0", "ws1-1"},
			},
			MigrationID: "migration-1",
			Status:      cluster.PartitionMigrationJobStatusNew,
		}

		t.Run("marshal unmarshal", func(t *testing.T) {
			data, err := jsonrs.Marshal(job)
			require.NoError(t, err)

			var unmarshaled cluster.PartitionMigrationJob
			err = jsonrs.Unmarshal(data, &unmarshaled)
			require.NoError(t, err)
			require.Equal(t, job, &unmarshaled)
		})
	})
}

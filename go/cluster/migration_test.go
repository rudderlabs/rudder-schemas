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

		t.Run("Clone", func(t *testing.T) {
			original := &cluster.PartitionMigration{
				ID:     "test-id",
				Status: cluster.PartitionMigrationStatusMigrating,
				Jobs: []*cluster.PartitionMigrationJobHeader{
					{
						JobID:      "job-1",
						SourceNode: 0,
						TargetNode: 1,
						Partitions: []string{"partition-1", "partition-2"},
					},
					{
						JobID:      "job-2",
						SourceNode: 2,
						TargetNode: 3,
						Partitions: []string{"partition-3"},
					},
				},
				AckKeyPrefix: "test-ack-prefix",
			}

			cloned := original.Clone()

			// Verify that the clone is not the same object
			require.NotSame(t, original, cloned)

			// Verify that the top-level fields are equal
			require.Equal(t, original, cloned)

			// Verify that the jobs are deeply copied
			for i := range original.Jobs {
				require.NotSame(t, original.Jobs[i], cloned.Jobs[i])
				require.Equal(t, original.Jobs[i], cloned.Jobs[i])
			}

			// Verify that modifying the clone doesn't affect the original
			cloned.ID = "modified-id"
			cloned.Jobs[0].JobID = "modified-job-id"
			cloned.Jobs[0].Partitions[0] = "modified-partition"

			require.Equal(t, "test-id", original.ID)
			require.Equal(t, "job-1", original.Jobs[0].JobID)
			require.Equal(t, "partition-1", original.Jobs[0].Partitions[0])
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

	t.Run("PartitionMigrationJobHeader", func(t *testing.T) {
		t.Run("Clone", func(t *testing.T) {
			original := &cluster.PartitionMigrationJobHeader{
				JobID:      "test-job-id",
				SourceNode: 1,
				TargetNode: 2,
				Partitions: []string{"partition-a", "partition-b", "partition-c"},
			}

			cloned := original.Clone()

			// Verify that the clone is not the same object
			require.NotSame(t, original, cloned)

			// Verify that all fields are equal
			require.Equal(t, original, cloned)

			// Verify that modifying the clone doesn't affect the original
			cloned.JobID = "modified-job-id"
			cloned.Partitions[0] = "modified-partition"

			require.Equal(t, "test-job-id", original.JobID)
			require.Equal(t, "partition-a", original.Partitions[0])
		})
	})
}

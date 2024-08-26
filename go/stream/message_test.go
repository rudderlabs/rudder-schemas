package stream_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

func TestMessage(t *testing.T) {
	t.Run("properties to/from: pulsar", func(t *testing.T) {
		input := map[string]string{
			"messageID":       "messageID",
			"routingKey":      "routingKey",
			"workspaceID":     "workspaceID",
			"userID":          "userID",
			"sourceID":        "sourceID",
			"destinationID":   "destinationID",
			"requestIP":       "10.29.13.20",
			"receivedAt":      time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC).Format(time.RFC3339Nano),
			"sourceJobRunID":  "sourceJobRunID",
			"sourceTaskRunID": "sourceTaskRunID",
			"traceID":         "traceID",
			"compression":     "some-serialized-compression-settings",
		}

		msg, err := stream.FromMapProperties(input)
		require.NoError(t, err)

		require.Equal(t, stream.MessageProperties{
			MessageID:       "messageID",
			RoutingKey:      "routingKey",
			WorkspaceID:     "workspaceID",
			UserID:          "userID",
			SourceID:        "sourceID",
			DestinationID:   "destinationID",
			RequestIP:       "10.29.13.20",
			ReceivedAt:      time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
			SourceJobRunID:  "sourceJobRunID",
			SourceTaskRunID: "sourceTaskRunID",
			TraceID:         "traceID",
			Compression:     "some-serialized-compression-settings",
		}, msg)

		propertiesOut := stream.ToMapProperties(msg)
		require.Equal(t, input, propertiesOut)

		t.Run("invalid receivedAt format", func(t *testing.T) {
			msg, err := stream.FromMapProperties(map[string]string{
				"receivedAt": time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC).Format(time.Kitchen),
			})
			require.Empty(t, msg)
			require.EqualError(t, err, `parsing receivedAt: parsing time "2:30AM" as "2006-01-02T15:04:05.999999999Z07:00": cannot parse "2:30AM" as "2006"`)
		})
	})

	t.Run("properties to/from: pulsar with webhook stage", func(t *testing.T) {
		input := map[string]string{
			"messageID":            "messageID",
			"routingKey":           "routingKey",
			"workspaceID":          "workspaceID",
			"userID":               "userID",
			"sourceID":             "sourceID",
			"destinationID":        "destinationID",
			"requestIP":            "10.29.13.20",
			"receivedAt":           time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC).Format(time.RFC3339Nano),
			"sourceJobRunID":       "sourceJobRunID",
			"sourceTaskRunID":      "sourceTaskRunID",
			"traceID":              "traceID",
			"sourceType":           "sourceType",
			"webhookFailureReason": "webhookFailureReason",
			"stage":                stream.StageWebhook,
			"compression":          "some-serialized-compression-settings",
		}

		msg, err := stream.FromMapProperties(input)
		require.NoError(t, err)

		require.Equal(t, stream.MessageProperties{
			MessageID:            "messageID",
			RoutingKey:           "routingKey",
			WorkspaceID:          "workspaceID",
			UserID:               "userID",
			SourceID:             "sourceID",
			DestinationID:        "destinationID",
			RequestIP:            "10.29.13.20",
			ReceivedAt:           time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
			SourceJobRunID:       "sourceJobRunID",
			SourceTaskRunID:      "sourceTaskRunID",
			TraceID:              "traceID",
			SourceType:           "sourceType",
			WebhookFailureReason: "webhookFailureReason",
			Stage:                stream.StageWebhook,
			Compression:          "some-serialized-compression-settings",
		}, msg)

		propertiesOut := stream.ToMapProperties(msg)
		require.Equal(t, input, propertiesOut)
	})

	t.Run("message to/from: JSON", func(t *testing.T) {
		input := `
		{
			"properties": {
				"messageID": "messageID",
				"routingKey": "routingKey",
				"workspaceID": "workspaceID",
				"userID": "userID",
				"sourceID": "sourceID",
				"destinationID": "destinationID",
				"receivedAt": "2024-08-01T02:30:50.0000002Z",
				"requestIP": "10.29.13.20",
				"sourceJobRunID": "sourceJobRunID",
				"sourceTaskRunID": "sourceTaskRunID",
				"traceID": "traceID",
				"compression": "some-serialized-compression-settings"
			},
			"payload": {
				"key": "value",
				"key2": "value2",
				"key3": {
					"key4": "value4"
				}
			}
		}`

		msg := stream.Message{}
		err := json.Unmarshal([]byte(input), &msg)
		require.NoError(t, err)
		require.Equal(t, stream.Message{
			Properties: stream.MessageProperties{
				MessageID:       "messageID",
				RoutingKey:      "routingKey",
				WorkspaceID:     "workspaceID",
				UserID:          "userID",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				RequestIP:       "10.29.13.20",
				ReceivedAt:      time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
				SourceJobRunID:  "sourceJobRunID",
				SourceTaskRunID: "sourceTaskRunID",
				TraceID:         "traceID",
				Compression:     "some-serialized-compression-settings",
			},
			Payload: json.RawMessage(`{
				"key": "value",
				"key2": "value2",
				"key3": {
					"key4": "value4"
				}
			}`),
		}, msg)

		output, err := json.Marshal(msg)
		require.NoError(t, err)
		require.JSONEq(t, input, string(output))
	})

	t.Run("message to/from: JSON with webhook stage", func(t *testing.T) {
		input := `
		{
			"properties": {
				"messageID": "messageID",
				"routingKey": "routingKey",
				"workspaceID": "workspaceID",
				"userID": "userID",
				"sourceID": "sourceID",
				"destinationID": "destinationID",
				"receivedAt": "2024-08-01T02:30:50.0000002Z",
				"requestIP": "10.29.13.20",
				"sourceJobRunID": "sourceJobRunID",
				"sourceTaskRunID": "sourceTaskRunID",
				"traceID": "traceID",
				"sourceType": "sourceType",
				"webhookFailureReason": "webhookFailureReason",
				"stage": "webhook",
				"compression": "some-serialized-compression-settings"
			},
			"payload": {
				"key": "value",
				"key2": "value2",
				"key3": {
					"key4": "value4"
				}
			}
		}`

		msg := stream.Message{}
		err := json.Unmarshal([]byte(input), &msg)
		require.NoError(t, err)
		require.Equal(t, stream.Message{
			Properties: stream.MessageProperties{
				MessageID:       "messageID",
				RoutingKey:      "routingKey",
				WorkspaceID:     "workspaceID",
				UserID:          "userID",
				SourceID:        "sourceID",
				DestinationID:   "destinationID",
				RequestIP:       "10.29.13.20",
				ReceivedAt:      time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
				SourceJobRunID:  "sourceJobRunID",
				SourceTaskRunID: "sourceTaskRunID",
				TraceID:         "traceID", SourceType: "sourceType",
				WebhookFailureReason: "webhookFailureReason",
				Stage:                stream.StageWebhook,
				Compression:          "some-serialized-compression-settings",
			},
			Payload: json.RawMessage(`{
				"key": "value",
				"key2": "value2",
				"key3": {
					"key4": "value4"
				}
			}`),
		}, msg)

		output, err := json.Marshal(msg)
		require.NoError(t, err)
		require.JSONEq(t, input, string(output))
	})

	t.Run("validation ok", func(t *testing.T) {
		validator := stream.NewMessageValidator()

		msg := stream.Message{
			Properties: stream.MessageProperties{
				MessageID:   "messageID",
				RoutingKey:  "routingKey",
				WorkspaceID: "workspaceID",
				SourceID:    "sourceID",
				ReceivedAt:  time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
				RequestIP:   "10.29.13.20",
				// missing optional:
				// UserID:      "userID",
				// SourceJobRunID:  "sourceJobRunID",
				// SourceTaskRunID: "sourceTaskRunID",
				// TraceID:         "traceID",
			},
			Payload: json.RawMessage(`{}`),
		}
		err := validator(&msg)
		require.NoError(t, err)
	})

	t.Run("validation Err", func(t *testing.T) {
		validator := stream.NewMessageValidator()

		msg := stream.Message{
			Properties: stream.MessageProperties{
				MessageID:   "",
				RoutingKey:  "routingKey",
				WorkspaceID: "workspaceID",
				SourceID:    "sourceID",
				RequestIP:   "10.29.13.20",
				ReceivedAt:  time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
			},
			Payload: json.RawMessage(`{}`),
		}

		err := validator(&msg)
		require.EqualError(t, err, "Key: 'Message.Properties.MessageID' Error:Field validation for 'MessageID' failed on the 'required' tag")
	})
}

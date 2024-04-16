package stream_test

import (
	"encoding/json"
	"testing"

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
			"sourceJobRunID":  "sourceJobRunID",
			"sourceTaskRunID": "sourceTaskRunID",
			"traceID":         "traceID",
		}

		inPayload := []byte(`{
			"key": "value",
			"key2": "value2",
			"key3": {
				"key4": "value4"
			}
		}`)

		msg, err := stream.FromPulsarMessage(input, inPayload)
		require.NoError(t, err)

		require.Equal(t, stream.Message{
			Properties: stream.MessageProperties{
				MessageID:       "messageID",
				RoutingKey:      "routingKey",
				WorkspaceID:     "workspaceID",
				UserID:          "userID",
				SourceID:        "sourceID",
				SourceJobRunID:  "sourceJobRunID",
				SourceTaskRunID: "sourceTaskRunID",
				TraceID:         "traceID",
			},
			Payload: inPayload,
		}, msg)

		propertiesOut, payloadOut := stream.ToPulsarMessage(msg)
		require.Equal(t, input, propertiesOut)
		require.Equal(t, inPayload, payloadOut)
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
				"sourceJobRunID": "sourceJobRunID",
				"sourceTaskRunID": "sourceTaskRunID",
				"traceID": "traceID"
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
				SourceJobRunID:  "sourceJobRunID",
				SourceTaskRunID: "sourceTaskRunID",
				TraceID:         "traceID",
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
				UserID:      "userID",
				SourceID:    "sourceID",
				// missing optional:
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
				// missing message ID:

				// MessageID:   "messageID",
				RoutingKey:  "routingKey",
				WorkspaceID: "workspaceID",
				UserID:      "userID",
				SourceID:    "sourceID",
			},
			Payload: json.RawMessage(`{}`),
		}

		err := validator(&msg)
		require.EqualError(t, err, "Key: 'Message.Properties.MessageID' Error:Field validation for 'MessageID' failed on the 'required' tag")
	})
}

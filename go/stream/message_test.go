package stream_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-schemas/go/stream"
)

func TestMessage(t *testing.T) {
	t.Run("properties to/from: pulsar", func(t *testing.T) {
		input := map[string]string{
			"requestType":     "requestType",
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
			"encryption":      "some-serialized-encryption-settings",
			"encryptionKeyID": "encryptionKeyID",
		}

		msg, err := stream.FromMapProperties(input)
		require.NoError(t, err)

		require.Equal(t, stream.MessageProperties{
			RequestType:     "requestType",
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
			Encryption:      "some-serialized-encryption-settings",
			EncryptionKeyID: "encryptionKeyID",
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
			"requestType":          "requestType",
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
			"encryption":           "some-serialized-encryption-settings",
			"encryptionKeyID":      "encryptionKeyID",
		}

		msg, err := stream.FromMapProperties(input)
		require.NoError(t, err)

		require.Equal(t, stream.MessageProperties{
			RequestType:          "requestType",
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
			Encryption:           "some-serialized-encryption-settings",
			EncryptionKeyID:      "encryptionKeyID",
		}, msg)

		propertiesOut := stream.ToMapProperties(msg)
		require.Equal(t, input, propertiesOut)
	})

	t.Run("properties to/from: pulsar with bot fields", func(t *testing.T) {
		t.Run("isBot is true", func(t *testing.T) {
			input := map[string]string{
				"requestType":         "requestType",
				"routingKey":          "routingKey",
				"workspaceID":         "workspaceID",
				"userID":              "userID",
				"sourceID":            "sourceID",
				"destinationID":       "destinationID",
				"requestIP":           "10.29.13.20",
				"receivedAt":          time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC).Format(time.RFC3339Nano),
				"sourceJobRunID":      "sourceJobRunID",
				"sourceTaskRunID":     "sourceTaskRunID",
				"traceID":             "traceID",
				"compression":         "some-serialized-compression-settings",
				"encryption":          "some-serialized-encryption-settings",
				"encryptionKeyID":     "encryptionKeyID",
				"isBot":               "true",
				"botName":             "TestBot",
				"botURL":              "https://testbot.com",
				"botIsInvalidBrowser": "true",
			}

			msg, err := stream.FromMapProperties(input)
			require.NoError(t, err)

			require.Equal(t, stream.MessageProperties{
				RequestType:         "requestType",
				RoutingKey:          "routingKey",
				WorkspaceID:         "workspaceID",
				UserID:              "userID",
				SourceID:            "sourceID",
				DestinationID:       "destinationID",
				RequestIP:           "10.29.13.20",
				ReceivedAt:          time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
				SourceJobRunID:      "sourceJobRunID",
				SourceTaskRunID:     "sourceTaskRunID",
				TraceID:             "traceID",
				Compression:         "some-serialized-compression-settings",
				Encryption:          "some-serialized-encryption-settings",
				EncryptionKeyID:     "encryptionKeyID",
				IsBot:               true,
				BotName:             "TestBot",
				BotURL:              "https://testbot.com",
				BotIsInvalidBrowser: true,
			}, msg)

			propertiesOut := stream.ToMapProperties(msg)
			require.Equal(t, input, propertiesOut)
		})

		t.Run("isBot is false", func(t *testing.T) {
			msg, err := stream.FromMapProperties(map[string]string{
				"receivedAt": time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC).Format(time.RFC3339Nano),
				"isBot":      "false",
			})
			require.NoError(t, err)

			require.False(t, msg.IsBot)
			require.Empty(t, msg.BotName)
			require.Empty(t, msg.BotURL)
			require.False(t, msg.BotIsInvalidBrowser)

			propertiesOut := stream.ToMapProperties(msg)

			require.NotContains(t, propertiesOut, "isBot")
			require.NotContains(t, propertiesOut, "botName")
			require.NotContains(t, propertiesOut, "botURL")
			require.NotContains(t, propertiesOut, "botIsInvalidBrowser")
		})

		t.Run("invalid isBot format", func(t *testing.T) {
			msg, err := stream.FromMapProperties(map[string]string{
				"receivedAt":          time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC).Format(time.RFC3339Nano),
				"isBot":               "not-a-boolean",
				"botIsInvalidBrowser": "true",
			})
			require.Empty(t, msg)
			require.EqualError(t, err, `parsing isBot: strconv.ParseBool: parsing "not-a-boolean": invalid syntax`)
		})

		t.Run("invalid botIsInvalidBrowser format", func(t *testing.T) {
			msg, err := stream.FromMapProperties(map[string]string{
				"receivedAt":          time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC).Format(time.RFC3339Nano),
				"isBot":               "true",
				"botIsInvalidBrowser": "not-a-boolean",
			})
			require.Empty(t, msg)
			require.EqualError(t, err, `parsing botIsInvalidBrowser: strconv.ParseBool: parsing "not-a-boolean": invalid syntax`)
		})
	})

	t.Run("message to/from: JSON", func(t *testing.T) {
		input := `
		{
			"properties": {
				"requestType": "requestType",
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
				"compression": "some-serialized-compression-settings",
				"encryption": "some-serialized-encryption-settings",
				"encryptionKeyID": "encryptionKeyID"
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
				RequestType:     "requestType",
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
				Encryption:      "some-serialized-encryption-settings",
				EncryptionKeyID: "encryptionKeyID",
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
				"requestType": "requestType",
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
				"compression": "some-serialized-compression-settings",
				"encryption": "some-serialized-encryption-settings",
				"encryptionKeyID": "encryptionKeyID"
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
				RequestType:     "requestType",
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
				Encryption:           "some-serialized-encryption-settings",
				EncryptionKeyID:      "encryptionKeyID",
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

	t.Run("message to/from: JSON with bot fields", func(t *testing.T) {
		input := `
		{
			"properties": {
				"requestType": "requestType",
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
				"isBot": true,
				"botName": "TestBot",
				"botURL": "https://testbot.com",
				"botIsInvalidBrowser": true
			},
			"payload": {
				"key": "value"
			}
		}`

		msg := stream.Message{}
		err := json.Unmarshal([]byte(input), &msg)
		require.NoError(t, err)
		require.Equal(t, stream.Message{
			Properties: stream.MessageProperties{
				RequestType:         "requestType",
				RoutingKey:          "routingKey",
				WorkspaceID:         "workspaceID",
				UserID:              "userID",
				SourceID:            "sourceID",
				DestinationID:       "destinationID",
				RequestIP:           "10.29.13.20",
				ReceivedAt:          time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
				SourceJobRunID:      "sourceJobRunID",
				SourceTaskRunID:     "sourceTaskRunID",
				TraceID:             "traceID",
				IsBot:               true,
				BotName:             "TestBot",
				BotURL:              "https://testbot.com",
				BotIsInvalidBrowser: true,
			},
			Payload: json.RawMessage(`{
				"key": "value"
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
				RequestType: "requestType",
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
				RequestType: "requestType",
				RoutingKey:  "routingKey",
				WorkspaceID: "",
				SourceID:    "sourceID",
				RequestIP:   "10.29.13.20",
				ReceivedAt:  time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
			},
			Payload: json.RawMessage(`{}`),
		}

		err := validator(&msg)
		require.EqualError(t, err, "Key: 'Message.Properties.WorkspaceID' Error:Field validation for 'WorkspaceID' failed on the 'required' tag")
	})

	t.Run("validation Err: with encryption properties", func(t *testing.T) {
		validator := stream.NewMessagePropertiesValidator(stream.WithEncryptionPropertiesValidator())
		msg := stream.Message{
			Properties: stream.MessageProperties{
				RequestType: "requestType",
				RoutingKey:  "routingKey",
				WorkspaceID: "workspace-id",
				SourceID:    "sourceID",
				RequestIP:   "10.29.13.20",
				ReceivedAt:  time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
				Encryption:  "some-serialized-encryption-settings",
			},
			Payload: json.RawMessage(`{}`),
		}

		err := validator(&msg.Properties)
		require.EqualError(t, err, "encryption key ID is required when encryption is set")
	})

	t.Run("validation ok: without encryption properties", func(t *testing.T) {
		validator := stream.NewMessagePropertiesValidator()
		msg := stream.Message{
			Properties: stream.MessageProperties{
				RequestType: "requestType",
				RoutingKey:  "routingKey",
				WorkspaceID: "workspace-id",
				SourceID:    "sourceID",
				RequestIP:   "10.29.13.20",
				ReceivedAt:  time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
				Encryption:  "some-serialized-encryption-settings",
			},
			Payload: json.RawMessage(`{}`),
		}

		err := validator(&msg.Properties)
		require.NoError(t, err)
	})

	t.Run("validation ok: encryption properties", func(t *testing.T) {
		validator := stream.NewMessagePropertiesValidator()

		msg := stream.Message{
			Properties: stream.MessageProperties{
				RequestType:     "requestType",
				RoutingKey:      "routingKey",
				WorkspaceID:     "workspace-id",
				SourceID:        "sourceID",
				RequestIP:       "10.29.13.20",
				ReceivedAt:      time.Date(2024, 8, 1, 0o2, 30, 50, 200, time.UTC),
				Encryption:      "some-serialized-encryption-settings",
				EncryptionKeyID: "encryption-key-id",
			},
			Payload: json.RawMessage(`{}`),
		}

		err := validator(&msg.Properties)
		require.NoError(t, err)
	})

	t.Run("logger fields - webhook stage", func(t *testing.T) {
		properties := stream.MessageProperties{
			RequestType:          "requestType",
			RoutingKey:           "routingKey",
			WorkspaceID:          "workspaceID",
			SourceID:             "sourceID",
			ReceivedAt:           time.Date(2024, 8, 1, 2, 30, 50, 200, time.UTC),
			RequestIP:            "10.29.13.20",
			DestinationID:        "destinationID",
			UserID:               "userID",
			SourceJobRunID:       "sourceJobRunID",
			SourceTaskRunID:      "sourceTaskRunID",
			TraceID:              "traceID",
			SourceType:           "sourceType",
			WebhookFailureReason: "webhookFailureReason",
			Stage:                stream.StageWebhook,
			Compression:          "some-serialized-compression-settings",
			Encryption:           "some-serialized-encryption-settings",
			EncryptionKeyID:      "encryptionKeyID",
		}

		expectedFields := []logger.Field{
			logger.NewStringField("requestType", "requestType"),
			logger.NewStringField("routingKey", "routingKey"),
			logger.NewStringField("workspaceID", "workspaceID"),
			logger.NewStringField("sourceID", "sourceID"),
			logger.NewStringField("destinationID", "destinationID"),
			logger.NewStringField("requestIP", "10.29.13.20"),
			logger.NewStringField("receivedAt", "2024-08-01T02:30:50.0000002Z"),
			logger.NewStringField("userID", "userID"),
			logger.NewStringField("sourceJobRunID", "sourceJobRunID"),
			logger.NewStringField("sourceTaskRunID", "sourceTaskRunID"),
			logger.NewStringField("traceID", "traceID"),
			logger.NewStringField("sourceType", "sourceType"),
			logger.NewStringField("webhookFailureReason", "webhookFailureReason"),
			logger.NewStringField("stage", "webhook"),
			logger.NewStringField("compression", "some-serialized-compression-settings"),
			logger.NewStringField("encryption", "some-serialized-encryption-settings"),
			logger.NewStringField("encryptionKeyID", "encryptionKeyID"),
			logger.NewBoolField("isBot", false),
		}

		require.ElementsMatch(t, expectedFields, properties.LoggerFields())
	})

	t.Run("logger fields - non-webhook stage", func(t *testing.T) {
		properties := stream.MessageProperties{
			RequestType:     "requestType",
			RoutingKey:      "routingKey",
			WorkspaceID:     "workspaceID",
			SourceID:        "sourceID",
			ReceivedAt:      time.Date(2024, 8, 1, 2, 30, 50, 200, time.UTC),
			RequestIP:       "10.29.13.20",
			DestinationID:   "destinationID",
			UserID:          "userID",
			SourceJobRunID:  "sourceJobRunID",
			SourceTaskRunID: "sourceTaskRunID",
			TraceID:         "traceID",
			SourceType:      "sourceType",
			Compression:     "some-serialized-compression-settings",
			Encryption:      "some-serialized-encryption-settings",
			EncryptionKeyID: "encryptionKeyID",
		}

		expectedFields := []logger.Field{
			logger.NewStringField("requestType", "requestType"),
			logger.NewStringField("routingKey", "routingKey"),
			logger.NewStringField("workspaceID", "workspaceID"),
			logger.NewStringField("sourceID", "sourceID"),
			logger.NewStringField("destinationID", "destinationID"),
			logger.NewStringField("requestIP", "10.29.13.20"),
			logger.NewStringField("receivedAt", "2024-08-01T02:30:50.0000002Z"),
			logger.NewStringField("userID", "userID"),
			logger.NewStringField("sourceJobRunID", "sourceJobRunID"),
			logger.NewStringField("sourceTaskRunID", "sourceTaskRunID"),
			logger.NewStringField("traceID", "traceID"),
			logger.NewStringField("compression", "some-serialized-compression-settings"),
			logger.NewStringField("encryption", "some-serialized-encryption-settings"),
			logger.NewStringField("encryptionKeyID", "encryptionKeyID"),
			logger.NewBoolField("isBot", false),
		}

		require.ElementsMatch(t, expectedFields, properties.LoggerFields())
	})

	t.Run("logger fields - bot fields", func(t *testing.T) {
		properties := stream.MessageProperties{
			RequestType:         "requestType",
			RoutingKey:          "routingKey",
			WorkspaceID:         "workspaceID",
			SourceID:            "sourceID",
			ReceivedAt:          time.Date(2024, 8, 1, 2, 30, 50, 200, time.UTC),
			RequestIP:           "10.29.13.20",
			DestinationID:       "destinationID",
			UserID:              "userID",
			SourceJobRunID:      "sourceJobRunID",
			SourceTaskRunID:     "sourceTaskRunID",
			TraceID:             "traceID",
			SourceType:          "sourceType",
			Compression:         "some-serialized-compression-settings",
			Encryption:          "some-serialized-encryption-settings",
			EncryptionKeyID:     "encryptionKeyID",
			IsBot:               true,
			BotName:             "TestBot",
			BotURL:              "https://testbot.com",
			BotIsInvalidBrowser: true,
		}

		expectedFields := []logger.Field{
			logger.NewStringField("requestType", "requestType"),
			logger.NewStringField("routingKey", "routingKey"),
			logger.NewStringField("workspaceID", "workspaceID"),
			logger.NewStringField("sourceID", "sourceID"),
			logger.NewStringField("destinationID", "destinationID"),
			logger.NewStringField("requestIP", "10.29.13.20"),
			logger.NewStringField("receivedAt", "2024-08-01T02:30:50.0000002Z"),
			logger.NewStringField("userID", "userID"),
			logger.NewStringField("sourceJobRunID", "sourceJobRunID"),
			logger.NewStringField("sourceTaskRunID", "sourceTaskRunID"),
			logger.NewStringField("traceID", "traceID"),
			logger.NewStringField("compression", "some-serialized-compression-settings"),
			logger.NewStringField("encryption", "some-serialized-encryption-settings"),
			logger.NewStringField("encryptionKeyID", "encryptionKeyID"),
			logger.NewBoolField("isBot", true),
			logger.NewStringField("botName", "TestBot"),
			logger.NewStringField("botURL", "https://testbot.com"),
			logger.NewBoolField("botIsInvalidBrowser", true),
		}

		require.ElementsMatch(t, expectedFields, properties.LoggerFields())
	})
}

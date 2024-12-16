package stream

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	StageWebhook = "webhook"

	mapKeyRequestType          = "requestType"
	mapKeyRoutingKey           = "routingKey"
	mapKeyWorkspaceID          = "workspaceID"
	mapKeySourceID             = "sourceID"
	mapKeyDestinationID        = "destinationID"
	mapKeyRequestIP            = "requestIP"
	mapKeyReceivedAt           = "receivedAt"
	mapKeyUserID               = "userID"
	mapKeySourceJobRunID       = "sourceJobRunID"
	mapKeySourceTaskRunID      = "sourceTaskRunID"
	mapKeyTraceID              = "traceID"
	mapKeySourceType           = "sourceType"
	mapKeyWebhookFailureReason = "webhookFailureReason"
	mapKeyStage                = "stage"
	mapKeyCompression          = "compression"
	mapKeyEncryption           = "encryption"
	mapKeyEncryptionKeyID      = "encryptionKeyID"
)

var (
	messagePropertiesDefaultSize      int
	messagePropertiesStageWebhookSize int
)

func init() {
	messagePropertiesDefaultSize = len(ToMapProperties(MessageProperties{}))
	messagePropertiesStageWebhookSize = len(ToMapProperties(MessageProperties{Stage: StageWebhook}))
}

type Message struct {
	Properties MessageProperties `json:"properties" validate:"required"`
	Payload    json.RawMessage   `json:"payload" validate:"required"`
}

type MessageProperties struct {
	RequestType          string    `json:"requestType,omitempty"` // optional, make it required in the next version
	RoutingKey           string    `json:"routingKey" validate:"required"`
	WorkspaceID          string    `json:"workspaceID" validate:"required"`
	SourceID             string    `json:"sourceID" validate:"required"`
	ReceivedAt           time.Time `json:"receivedAt" validate:"required"`
	RequestIP            string    `json:"requestIP" validate:"required"`
	DestinationID        string    `json:"destinationID,omitempty"`        // optional
	UserID               string    `json:"userID,omitempty"`               // optional
	SourceJobRunID       string    `json:"sourceJobRunID,omitempty"`       // optional
	SourceTaskRunID      string    `json:"sourceTaskRunID,omitempty"`      // optional
	TraceID              string    `json:"traceID,omitempty"`              // optional
	SourceType           string    `json:"sourceType,omitempty"`           // optional
	WebhookFailureReason string    `json:"webhookFailureReason,omitempty"` // optional
	Stage                string    `json:"stage,omitempty"`                // optional
	Compression          string    `json:"compression,omitempty"`          // optional
	Encryption           string    `json:"encryption,omitempty"`           // optional
	// if key is rotated EncryptionKeyID should be used to refer to correct key
	EncryptionKeyID string `json:"encryptionKeyID,omitempty"` // optional
}

func (m MessageProperties) LoggerFields() []logger.Field {
	var fields []logger.Field

	if m.Stage == StageWebhook {
		fields = make([]logger.Field, 0, messagePropertiesStageWebhookSize)
		fields = append(fields, logger.NewStringField(mapKeySourceType, m.SourceType))
		fields = append(fields, logger.NewStringField(mapKeyWebhookFailureReason, m.WebhookFailureReason))
		fields = append(fields, logger.NewStringField(mapKeyStage, m.Stage))
	} else {
		fields = make([]logger.Field, 0, messagePropertiesDefaultSize)
	}

	fields = append(fields, logger.NewStringField(mapKeyRequestType, m.RequestType))
	fields = append(fields, logger.NewStringField(mapKeyRoutingKey, m.RoutingKey))
	fields = append(fields, logger.NewStringField(mapKeyWorkspaceID, m.WorkspaceID))
	fields = append(fields, logger.NewStringField(mapKeyUserID, m.UserID))
	fields = append(fields, logger.NewStringField(mapKeySourceID, m.SourceID))
	fields = append(fields, logger.NewStringField(mapKeyDestinationID, m.DestinationID))
	fields = append(fields, logger.NewStringField(mapKeyRequestIP, m.RequestIP))
	fields = append(fields, logger.NewStringField(mapKeyReceivedAt, m.ReceivedAt.Format(time.RFC3339Nano)))
	fields = append(fields, logger.NewStringField(mapKeySourceJobRunID, m.SourceJobRunID))
	fields = append(fields, logger.NewStringField(mapKeySourceTaskRunID, m.SourceTaskRunID))
	fields = append(fields, logger.NewStringField(mapKeyTraceID, m.TraceID))
	fields = append(fields, logger.NewStringField(mapKeyCompression, m.Compression))
	fields = append(fields, logger.NewStringField(mapKeyEncryption, m.Encryption))
	fields = append(fields, logger.NewStringField(mapKeyEncryptionKeyID, m.EncryptionKeyID))

	return fields
}

// FromMapProperties converts a property map to MessageProperties.
func FromMapProperties(properties map[string]string) (MessageProperties, error) {
	receivedAt, err := time.Parse(time.RFC3339Nano, properties[mapKeyReceivedAt])
	if err != nil {
		return MessageProperties{}, fmt.Errorf("parsing receivedAt: %w", err)
	}

	return MessageProperties{
		RequestType:          properties[mapKeyRequestType],
		RoutingKey:           properties[mapKeyRoutingKey],
		WorkspaceID:          properties[mapKeyWorkspaceID],
		RequestIP:            properties[mapKeyRequestIP],
		UserID:               properties[mapKeyUserID],
		SourceID:             properties[mapKeySourceID],
		DestinationID:        properties[mapKeyDestinationID],
		ReceivedAt:           receivedAt,
		SourceJobRunID:       properties[mapKeySourceJobRunID],
		SourceTaskRunID:      properties[mapKeySourceTaskRunID],
		TraceID:              properties[mapKeyTraceID],
		SourceType:           properties[mapKeySourceType],
		WebhookFailureReason: properties[mapKeyWebhookFailureReason],
		Stage:                properties[mapKeyStage],
		Compression:          properties[mapKeyCompression],
		Encryption:           properties[mapKeyEncryption],
		EncryptionKeyID:      properties[mapKeyEncryptionKeyID],
	}, nil
}

// ToMapProperties converts a Message to map properties.
func ToMapProperties(properties MessageProperties) map[string]string {
	m := map[string]string{
		mapKeyRequestType:     properties.RequestType,
		mapKeyRoutingKey:      properties.RoutingKey,
		mapKeyWorkspaceID:     properties.WorkspaceID,
		mapKeyUserID:          properties.UserID,
		mapKeySourceID:        properties.SourceID,
		mapKeyDestinationID:   properties.DestinationID,
		mapKeyRequestIP:       properties.RequestIP,
		mapKeyReceivedAt:      properties.ReceivedAt.Format(time.RFC3339Nano),
		mapKeySourceJobRunID:  properties.SourceJobRunID,
		mapKeySourceTaskRunID: properties.SourceTaskRunID,
		mapKeyTraceID:         properties.TraceID,
		mapKeyCompression:     properties.Compression,
		mapKeyEncryption:      properties.Encryption,
		mapKeyEncryptionKeyID: properties.EncryptionKeyID,
	}
	if properties.Stage == StageWebhook {
		m[mapKeySourceType] = properties.SourceType
		m[mapKeyWebhookFailureReason] = properties.WebhookFailureReason
		m[mapKeyStage] = properties.Stage
	}
	return m
}

func NewMessageValidator() func(msg *Message) error {
	validate := validator.New(validator.WithRequiredStructEnabled())
	return func(msg *Message) error {
		return validate.Struct(msg)
	}
}

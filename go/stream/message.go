package stream

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
)

const (
	StageWebhook = "webhook"

	mapKeyMessageID       = "messageID"
	mapKeyRoutingKey      = "routingKey"
	mapKeyWorkspaceID     = "workspaceID"
	mapKeySourceID        = "sourceID"
	mapKeyDestinationID   = "destinationID"
	mapKeyRequestIP       = "requestIP"
	mapKeyReceivedAt      = "receivedAt"
	mapKeyUserID          = "userID"
	mapKeySourceJobRunID  = "sourceJobRunID"
	mapKeySourceTaskRunID = "sourceTaskRunID"
	mapKeyTraceID         = "traceID"
	mapKeySourceType      = "sourceType"
	mapKeyReason          = "reason"
	mapKeyStage           = "stage"
)

type Message struct {
	Properties MessageProperties `json:"properties" validate:"required"`
	Payload    json.RawMessage   `json:"payload" validate:"required"`
}

type MessageProperties struct {
	MessageID            string    `json:"messageID" validate:"required"`
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
}

// FromMapProperties converts a property map to MessageProperties.
func FromMapProperties(properties map[string]string) (MessageProperties, error) {
	receivedAt, err := time.Parse(time.RFC3339Nano, properties[mapKeyReceivedAt])
	if err != nil {
		return MessageProperties{}, fmt.Errorf("parsing receivedAt: %w", err)
	}

	return MessageProperties{
		MessageID:            properties[mapKeyMessageID],
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
		WebhookFailureReason: properties[mapKeyReason],
		Stage:                properties[mapKeyStage],
	}, nil
}

// ToMapProperties converts a Message to map properties.
func ToMapProperties(properties MessageProperties) map[string]string {
	m := map[string]string{
		mapKeyMessageID:       properties.MessageID,
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
	}
	if properties.Stage == StageWebhook {
		m[mapKeySourceType] = properties.SourceType
		m[mapKeyReason] = properties.WebhookFailureReason
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

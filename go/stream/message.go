package stream

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
)

const (
	pulsarKeyMessageID       = "messageID"
	pulsarKeyRoutingKey      = "routingKey"
	pulsarKeyWorkspaceID     = "workspaceID"
	pulsarKeySourceID        = "sourceID"
	pulsarKeyUserID          = "userID"
	pulsarKeyReceivedAt      = "receivedAt"
	pulsarKeySourceJobRunID  = "sourceJobRunID"
	pulsarKeySourceTaskRunID = "sourceTaskRunID"
	pulsarKeyTraceID         = "traceID"
)

type Message struct {
	Properties MessageProperties `json:"properties" validate:"required"`
	Payload    json.RawMessage   `json:"payload" validate:"required"`
}

type MessageProperties struct {
	MessageID       string    `json:"messageID" validate:"required"`
	RoutingKey      string    `json:"routingKey" validate:"required"`
	WorkspaceID     string    `json:"workspaceID" validate:"required"`
	UserID          string    `json:"userID" validate:"required"`
	SourceID        string    `json:"sourceID" validate:"required"`
	ReceivedAt      time.Time `json:"receivedAt" validate:"required"`
	SourceJobRunID  string    `json:"sourceJobRunID,omitempty"`  // optional
	SourceTaskRunID string    `json:"sourceTaskRunID,omitempty"` // optional
	TraceID         string    `json:"traceID,omitempty"`         // optional
}

// FromMapProperties converts a property map to MessageProperties.
func FromMapProperties(properties map[string]string) (MessageProperties, error) {
	receivedAt, err := time.Parse(time.RFC3339Nano, properties[pulsarKeyReceivedAt])
	if err != nil {
		return MessageProperties{}, fmt.Errorf("parsing receivedAt: %w", err)
	}

	return MessageProperties{
		MessageID:       properties[pulsarKeyMessageID],
		RoutingKey:      properties[pulsarKeyRoutingKey],
		WorkspaceID:     properties[pulsarKeyWorkspaceID],
		UserID:          properties[pulsarKeyUserID],
		SourceID:        properties[pulsarKeySourceID],
		ReceivedAt:      receivedAt,
		SourceJobRunID:  properties[pulsarKeySourceJobRunID],
		SourceTaskRunID: properties[pulsarKeySourceTaskRunID],
		TraceID:         properties[pulsarKeyTraceID],
	}, nil
}

// ToMapProperties converts a Message to map properties.
func ToMapProperties(properties MessageProperties) map[string]string {
	return map[string]string{
		pulsarKeyMessageID:       properties.MessageID,
		pulsarKeyRoutingKey:      properties.RoutingKey,
		pulsarKeyWorkspaceID:     properties.WorkspaceID,
		pulsarKeyUserID:          properties.UserID,
		pulsarKeySourceID:        properties.SourceID,
		pulsarKeyReceivedAt:      properties.ReceivedAt.Format(time.RFC3339Nano),
		pulsarKeySourceJobRunID:  properties.SourceJobRunID,
		pulsarKeySourceTaskRunID: properties.SourceTaskRunID,
		pulsarKeyTraceID:         properties.TraceID,
	}
}

func NewMessageValidator() func(msg *Message) error {
	validate := validator.New(validator.WithRequiredStructEnabled())

	return func(msg *Message) error {
		return validate.Struct(msg)
	}
}

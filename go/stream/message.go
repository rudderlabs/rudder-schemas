package stream

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
)

const (
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
	Properties Properties      `json:"properties" validate:"required"`
	Payload    json.RawMessage `json:"payload" validate:"required"`
}

type Properties interface {
	FromMap(map[string]string) error
	ToMap() map[string]string
}

type MessageProperties struct {
	MessageID       string    `json:"messageID" validate:"required"`
	RoutingKey      string    `json:"routingKey" validate:"required"`
	WorkspaceID     string    `json:"workspaceID" validate:"required"`
	SourceID        string    `json:"sourceID" validate:"required"`
	ReceivedAt      time.Time `json:"receivedAt" validate:"required"`
	RequestIP       string    `json:"requestIP" validate:"required"`
	DestinationID   string    `json:"destinationID,omitempty"`   // optional
	UserID          string    `json:"userID,omitempty"`          // optional
	SourceJobRunID  string    `json:"sourceJobRunID,omitempty"`  // optional
	SourceTaskRunID string    `json:"sourceTaskRunID,omitempty"` // optional
	TraceID         string    `json:"traceID,omitempty"`         // optional
}

// FromMap populates MessageProperties from a map.
func (m *MessageProperties) FromMap(properties map[string]string) error {
	receivedAt, err := time.Parse(time.RFC3339Nano, properties[mapKeyReceivedAt])
	if err != nil {
		return fmt.Errorf("parsing receivedAt: %w", err)
	}

	m.MessageID = properties[mapKeyMessageID]
	m.RoutingKey = properties[mapKeyRoutingKey]
	m.WorkspaceID = properties[mapKeyWorkspaceID]
	m.RequestIP = properties[mapKeyRequestIP]
	m.UserID = properties[mapKeyUserID]
	m.SourceID = properties[mapKeySourceID]
	m.DestinationID = properties[mapKeyDestinationID]
	m.ReceivedAt = receivedAt
	m.SourceJobRunID = properties[mapKeySourceJobRunID]
	m.SourceTaskRunID = properties[mapKeySourceTaskRunID]
	m.TraceID = properties[mapKeyTraceID]

	return nil
}

// ToMap converts MessageProperties to a map.
func (m MessageProperties) ToMap() map[string]string {
	return map[string]string{
		mapKeyMessageID:       m.MessageID,
		mapKeyRoutingKey:      m.RoutingKey,
		mapKeyWorkspaceID:     m.WorkspaceID,
		mapKeyUserID:          m.UserID,
		mapKeySourceID:        m.SourceID,
		mapKeyDestinationID:   m.DestinationID,
		mapKeyRequestIP:       m.RequestIP,
		mapKeyReceivedAt:      m.ReceivedAt.Format(time.RFC3339Nano),
		mapKeySourceJobRunID:  m.SourceJobRunID,
		mapKeySourceTaskRunID: m.SourceTaskRunID,
		mapKeyTraceID:         m.TraceID,
	}
}

type WebhookProperties struct {
	WorkspaceID string `json:"workspaceID" validate:"required"`
	SourceID    string `json:"sourceID" validate:"required"`
	SourceType  string `json:"sourceType,omitempty" validate:"required"`
	Reason      string `json:"reason,omitempty" validate:"required"`
	Stage       string `json:"stage,omitempty" validate:"required"`
}

// FromMap populates WebhookProperties from a map.
func (w *WebhookProperties) FromMap(properties map[string]string) error {
	w.WorkspaceID = properties[mapKeyWorkspaceID]
	w.SourceID = properties[mapKeySourceID]
	w.SourceType = properties[mapKeySourceType]
	w.Reason = properties[mapKeyReason]
	w.Stage = properties[mapKeyStage]

	return nil
}

// ToMap converts WebhookProperties to a map.
func (w WebhookProperties) ToMap() map[string]string {
	return map[string]string{
		mapKeyWorkspaceID: w.WorkspaceID,
		mapKeySourceID:    w.SourceID,
		mapKeySourceType:  w.SourceType,
		mapKeyReason:      w.Reason,
		mapKeyStage:       w.Stage,
	}
}

// FromMapProperties populates a Properties object from a map.
func FromMapProperties(properties map[string]string, prop Properties) error {
	return prop.FromMap(properties)
}

// ToMapProperties converts a Properties object to a map.
func ToMapProperties(properties Properties) map[string]string {
	return properties.ToMap()
}

// NewMessageValidator creates a new validator for Message.
func NewMessageValidator() func(msg *Message) error {
	validate := validator.New(validator.WithRequiredStructEnabled())

	return func(msg *Message) error {
		return validate.Struct(msg)
	}
}

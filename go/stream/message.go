package stream

import (
	"encoding/json"

	"github.com/go-playground/validator/v10"
)

const (
	pulsarKeyMessageID       = "messageID"
	pulsarKeyRoutingKey      = "routingKey"
	pulsarKeyWorkspaceID     = "workspaceID"
	pulsarKeySourceID        = "sourceID"
	pulsarKeyUserID          = "userID"
	pulsarKeySourceJobRunID  = "sourceJobRunID"
	pulsarKeySourceTaskRunID = "sourceTaskRunID"
	pulsarKeyTraceID         = "traceID"
)

type Message struct {
	Properties MessageProperties `json:"properties"`
	Payload    json.RawMessage   `json:"payload"`
}

type MessageProperties struct {
	MessageID       string `json:"messageID" validate:"required"`
	RoutingKey      string `json:"routingKey" validate:"required"`
	WorkspaceID     string `json:"workspaceID" validate:"required"`
	UserID          string `json:"userID" validate:"required"`
	SourceID        string `json:"sourceID" validate:"required"`
	SourceJobRunID  string `json:"sourceJobRunID"`  // optional
	SourceTaskRunID string `json:"sourceTaskRunID"` // optional
	TraceID         string `json:"traceID"`         // optional
}

// FromPulsarMessage converts a Pulsar message to a Message.
func FromPulsarMessage(properties map[string]string, payload []byte) (Message, error) {
	return Message{
		Properties: MessageProperties{
			MessageID:       properties[pulsarKeyMessageID],
			RoutingKey:      properties[pulsarKeyRoutingKey],
			WorkspaceID:     properties[pulsarKeyWorkspaceID],
			UserID:          properties[pulsarKeyUserID],
			SourceID:        properties[pulsarKeySourceID],
			SourceJobRunID:  properties[pulsarKeySourceJobRunID],
			SourceTaskRunID: properties[pulsarKeySourceTaskRunID],
			TraceID:         properties[pulsarKeyTraceID],
		},
		Payload: json.RawMessage(payload),
	}, nil
}

// ToPulsarMessage converts a Message to a Pulsar message.
func ToPulsarMessage(msg Message) (map[string]string, []byte) {
	properties := msg.Properties
	return map[string]string{
		pulsarKeyMessageID:       properties.MessageID,
		pulsarKeyRoutingKey:      properties.RoutingKey,
		pulsarKeyWorkspaceID:     properties.WorkspaceID,
		pulsarKeySourceID:        properties.SourceID,
		pulsarKeyUserID:          properties.UserID,
		pulsarKeySourceJobRunID:  properties.SourceJobRunID,
		pulsarKeySourceTaskRunID: properties.SourceTaskRunID,
		pulsarKeyTraceID:         properties.TraceID,
	}, []byte(msg.Payload)
}

func NewMessageValidator() func(msg *Message) error {
	validate := validator.New(validator.WithRequiredStructEnabled())

	return func(msg *Message) error {
		return validate.Struct(msg)
	}
}

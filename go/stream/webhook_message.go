package stream

import (
	"encoding/json"
	"github.com/go-playground/validator/v10"
)

const (
	mapKeySourceType = "sourceType"
	mapKeyReason     = "reason"
	mapKeyStage      = "stage"
)

type WebhookMessage struct {
	Properties WebhookMessageProperties `json:"properties" validate:"required"`
	Payload    json.RawMessage          `json:"payload" validate:"required"`
}

type WebhookMessageProperties struct {
	WorkspaceID string `json:"workspaceID" validate:"required"`
	SourceID    string `json:"sourceID" validate:"required"`
	SourceType  string `json:"sourceType,omitempty" validate:"required"`
	Reason      string `json:"reason,omitempty" validate:"required"`
	Stage       string `json:"stage,omitempty" validate:"required"`
}

// FromWebhookMapProperties converts a property map to MessageProperties.
func FromWebhookMapProperties(properties map[string]string) (WebhookMessageProperties, error) {
	return WebhookMessageProperties{
		WorkspaceID: properties[mapKeyWorkspaceID],
		SourceID:    properties[mapKeySourceID],
		SourceType:  properties[mapKeySourceType],
		Reason:      properties[mapKeyReason],
		Stage:       properties[mapKeyStage],
	}, nil
}

// ToWebhookMapProperties converts a Message to map properties.
func ToWebhookMapProperties(properties WebhookMessageProperties) map[string]string {
	return map[string]string{
		mapKeyWorkspaceID: properties.WorkspaceID,
		mapKeySourceID:    properties.SourceID,
		mapKeySourceType:  properties.SourceType,
		mapKeyReason:      properties.Reason,
		mapKeyStage:       properties.Stage,
	}
}

func NewWebhookMessageValidator() func(msg *WebhookMessage) error {
	validate := validator.New(validator.WithRequiredStructEnabled())

	return func(msg *WebhookMessage) error {
		return validate.Struct(msg)
	}
}

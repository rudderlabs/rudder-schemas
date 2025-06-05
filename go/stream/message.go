package stream

import (
	"encoding/json"
	"fmt"
	"strconv"
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
	mapKeyIsBot                = "isBot"
	mapKeyBotName              = "botName"
	mapKeyBotURL               = "botURL"
	mapKeyBotIsInvalidBrowser  = "botIsInvalidBrowser"
	mapKeyNeedsBotEnrichment   = "needsBotEnrichment"
)

var (
	messagePropertiesDefaultSize      = len(ToMapProperties(MessageProperties{}))
	messagePropertiesStageWebhookSize = len(ToMapProperties(MessageProperties{Stage: StageWebhook}))
)

type Message struct {
	Properties MessageProperties `json:"properties" validate:"required"`
	Payload    json.RawMessage   `json:"payload" validate:"required"`
}

type MessageProperties struct {
	RequestType          string    `json:"requestType" validate:"required"`
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
	IsBot           bool   `json:"isBot,omitempty"`           // optional
	// BotName is the name of the bot that sent the event
	BotName string `json:"botName,omitempty"` // optional
	// BotURL contains the source URL or reference that explains why the user agent was identified as a bot
	BotURL string `json:"botURL,omitempty"` // optional
	// BotIsInvalidBrowser is true if event is a bot and the browser is invalid
	BotIsInvalidBrowser bool `json:"botIsInvalidBrowser,omitempty"` // optional
	// NeedsBotEnrichment is true if event should be enriched with bot details
	NeedsBotEnrichment bool `json:"needsBotEnrichment,omitempty"` // optional
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
	fields = append(fields, logger.NewBoolField(mapKeyIsBot, m.IsBot))
	if m.IsBot {
		fields = append(fields, logger.NewStringField(mapKeyBotName, m.BotName))
		fields = append(fields, logger.NewStringField(mapKeyBotURL, m.BotURL))
		fields = append(fields, logger.NewBoolField(mapKeyBotIsInvalidBrowser, m.BotIsInvalidBrowser))
		fields = append(fields, logger.NewBoolField(mapKeyNeedsBotEnrichment, m.NeedsBotEnrichment))
	}
	return fields
}

// FromMapProperties converts a property map to MessageProperties.
func FromMapProperties(properties map[string]string) (MessageProperties, error) {
	receivedAt, err := time.Parse(time.RFC3339Nano, properties[mapKeyReceivedAt])
	if err != nil {
		return MessageProperties{}, fmt.Errorf("parsing receivedAt: %w", err)
	}

	var isBot, botIsInvalidBrowser, needsBotEnrichment bool
	var botName, botURL string

	if properties[mapKeyIsBot] != "" {
		isBot, err = strconv.ParseBool(properties[mapKeyIsBot])
		if err != nil {
			return MessageProperties{}, fmt.Errorf("parsing isBot: %w", err)
		}
	}

	if isBot {
		botName = properties[mapKeyBotName]
		botURL = properties[mapKeyBotURL]

		if properties[mapKeyBotIsInvalidBrowser] != "" {
			botIsInvalidBrowser, err = strconv.ParseBool(properties[mapKeyBotIsInvalidBrowser])
			if err != nil {
				return MessageProperties{}, fmt.Errorf("parsing botIsInvalidBrowser: %w", err)
			}
		}

		if properties[mapKeyNeedsBotEnrichment] != "" {
			needsBotEnrichment, err = strconv.ParseBool(properties[mapKeyNeedsBotEnrichment])
			if err != nil {
				return MessageProperties{}, fmt.Errorf("parsing needsBotEnrichment: %w", err)
			}
		}
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
		IsBot:                isBot,
		BotName:              botName,
		BotURL:               botURL,
		BotIsInvalidBrowser:  botIsInvalidBrowser,
		NeedsBotEnrichment:   needsBotEnrichment,
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
	if properties.IsBot {
		m[mapKeyIsBot] = "true"
		m[mapKeyBotName] = properties.BotName
		m[mapKeyBotURL] = properties.BotURL
		m[mapKeyBotIsInvalidBrowser] = strconv.FormatBool(properties.BotIsInvalidBrowser)
		m[mapKeyNeedsBotEnrichment] = strconv.FormatBool(properties.NeedsBotEnrichment)
	}
	return m
}

func NewMessageValidator() func(msg *Message) error {
	validate := validator.New(validator.WithRequiredStructEnabled())
	return func(msg *Message) error {
		return validate.Struct(msg)
	}
}

func NewMessagePropertiesValidator(opt ...func(properties *MessageProperties) error) func(properties *MessageProperties) error {
	validate := validator.New(validator.WithRequiredStructEnabled())
	return func(properties *MessageProperties) error {
		for _, o := range opt {
			if err := o(properties); err != nil {
				return err
			}
		}
		return validate.Struct(properties)
	}
}

func WithEncryptionPropertiesValidator() func(properties *MessageProperties) error {
	return func(properties *MessageProperties) error {
		if properties.Encryption != "" && properties.EncryptionKeyID == "" {
			return fmt.Errorf("encryption key ID is required when encryption is set")
		}
		return nil
	}
}

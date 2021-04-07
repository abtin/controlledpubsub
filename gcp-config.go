package gcpconfig

import (
	"errors"
	"fmt"
	"os"
)

type GcpConfig struct {
	credentials  string
	projectID    string
	dataTopicID  string
	subscription string
}

// NewGcpConfig creates a new GcpConfig from the environment variables
func NewGcpConfig() (GcpConfig, error) {
	var errMsg string
	credentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credentials == "" {
		errMsg += "GOOGLE_APPLICATION_CREDENTIALS\n"
	}
	projectID := os.Getenv("GOOGLE_PROJECT_ID")
	if projectID == "" {
		errMsg += "GOOGLE_PROJECT_ID\n"
	}
	dataTopicID := os.Getenv("DATA_TOPIC_ID")
	if dataTopicID == "" {
		errMsg += "DATA_TOPIC_ID\n"
	}

	subscription := os.Getenv("DATA_TOPIC_SUBSCRIPTION")
	if subscription == "" {
		errMsg += "DATA_TOPIC_SUBSCRIPTION\n"
	}

	if errMsg != "" {
		return GcpConfig{}, errors.New(fmt.Sprintf("Please set the following Environment Variables:\n%s", errMsg))
	}
	return GcpConfig{
		credentials:  credentials,
		projectID:    projectID,
		dataTopicID:  dataTopicID,
		subscription: subscription,
	}, nil
}

// Credentials returns the GCP credential file location
func (c GcpConfig) Credentials() string {
	return c.credentials
}

// ProjectID returns the GCP Project ID
func (c GcpConfig) ProjectID() string {
	return c.projectID
}

// DataTopicID returns the GCP pubsub topic
func (c GcpConfig) DataTopicID() string {
	return c.dataTopicID
}

// DataTopicID returns the GCP pubsub topic
func (c GcpConfig) Subscription() string {
	return c.subscription
}

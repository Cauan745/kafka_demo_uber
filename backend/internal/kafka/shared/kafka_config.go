// Package shared
package shared

type KafkaConfig struct {
	Topic         string
	ConsumerGroup string
	Host          string
}

func NewKafkaConfig(topic string, consumerGroup string, host string) *KafkaConfig {
	return &KafkaConfig{
		Topic:         topic,
		ConsumerGroup: consumerGroup,
		Host:          host,
	}
}

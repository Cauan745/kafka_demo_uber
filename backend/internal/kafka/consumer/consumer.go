// Package consumer
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cauan745/trabalho_kafka/internal/kafka/shared"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topic    string
	msgCH    chan<- string
	readyCH  chan struct{}
	isReady  bool
	logger   *slog.Logger
}

func NewKafkaConsumer(msgCH chan<- string, logger *slog.Logger, cfg shared.KafkaConfig) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Host,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}

	consumer := &KafkaConsumer{
		consumer: c,
		topic:    cfg.Topic,
		msgCH:    msgCH,
		readyCH:  make(chan struct{}),
		isReady:  false,
		logger:   logger,
	}
	err = consumer.initializeKafkaTopic(cfg.Host, cfg.Topic)
	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics([]string{cfg.Topic}, nil)
	if err != nil {
		return nil, err
	}

	go consumer.checkReadyToAccept()
	go consumer.readMsgLoop()

	return consumer, nil
}

// Infinitely loops calling consumer.ReadMessage() which waits for a message for 1 second
// If errors occur or no message found will just try again
// If finds a message writes to c.msgCH
func (c *KafkaConsumer) readMsgLoop() {
	defer c.consumer.Close()

	// for {} = Infinite Loop
	for {
		// it will block and wait for up to 1 second (timeout) for a message to arrive
		// if a message arrives, it gets stored in msg.
		msg, err := c.consumer.ReadMessage(time.Second)
		// If no message arrives within that second, err will contain a timeout error.
		if err != nil && err.(kafka.Error).IsTimeout() {
			continue
		}
		if err != nil && !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			c.logger.Error(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
			continue
		}

		// extracts the raw byte data of the message

		payload := msg.Value
		c.msgCH <- string(payload)
	}
}

func (c *KafkaConsumer) initializeKafkaTopic(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	c.logger.Info(fmt.Sprintf("Creating topic '%s'...", topicName))
	topicSpec := kafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		return err
	}

	for _, result := range results {
		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			c.logger.Info(fmt.Sprintf("Topic already exists: %v", result.Error))
			continue
		}
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to create topic: %v", result.Error)
		}
		c.logger.Info(fmt.Sprintf("Topic '%s' created successfully", result.Topic))
	}

	return c.waitForTopicReady(brokers, topicName)
}

func (c *KafkaConsumer) waitForTopicReady(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	for {
		time.Sleep(1 * time.Second)
		metadata, err := adminClient.GetMetadata(&topicName, false, 5000)
		if err != nil {
			c.logger.Error(fmt.Sprintf("Metadata fetch failed %v\n", err))
			continue
		}

		topicMeta, exists := metadata.Topics[topicName]
		if !exists {
			continue
		}

		if len(topicMeta.Partitions) > 0 {
			allPartitionsReady := true
			for _, partition := range topicMeta.Partitions {
				if partition.Error.Code() != kafka.ErrNoError {
					allPartitionsReady = false
					break
				}
				if partition.Leader == -1 {
					allPartitionsReady = false
					break
				}
			}

			c.logger.Info("Cosumer Topic", "IS_INITIALIZED", allPartitionsReady)

			if allPartitionsReady {
				return nil
			}
		}
	}
}

// Loops sleeping for 1 second till receives a message from the c.readyCH
// or c.readyCheck() returns true, if returns false tries again
// if errors occur still sets c.isReady to true (why?)
func (c *KafkaConsumer) checkReadyToAccept() error {
	defer func() {
		c.isReady = true
	}()
	for {
		// If there is no message waiting on c.readyCH, the select statement immediately drops
		// into the default block instead of getting stuck waiting.
		select {
		case <-c.readyCH:
			return nil
		default:
			time.Sleep(1 * time.Second)
			isReady, err := c.readyCheck()
			if err != nil {
				c.logger.Error("Error on consumer readycheck")
				return err
			}
			c.logger.Warn("Consumer ready to accept", "STATUS", isReady)

			// Checks if there's at leasts one assignment (read readyCheck), if not the for then loops again
			if isReady {
				return nil
			}
		}
	}
}

// returns true if finds at least one assignment (one partition assigned to this consumer)
func (c *KafkaConsumer) readyCheck() (bool, error) {
	assignment, err := c.consumer.Assignment()
	if err != nil {
		c.logger.Error(fmt.Sprintf("Failed to get assignment: %v", err))
		return false, err
	}

	return len(assignment) > 0, nil
}

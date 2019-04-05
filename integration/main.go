package pubsubIntegration

import (
  "time"
  "context"
  googlePubsub "cloud.google.com/go/pubsub"
  "github.com/renra/go-errtrace/errtrace"
)

const (
  TopicDoesNotExist = "Topic does not exist"
)

type Topic interface{
  WrappedTopic() *googlePubsub.Topic
  Publish(context.Context, *googlePubsub.Message) *googlePubsub.PublishResult
  Exists(context.Context) (bool, *errtrace.Error)
  Delete(context.Context) (*errtrace.Error)
  Stop()
}

type Subscription interface{
  Receive(context.Context, func(context.Context, *googlePubsub.Message)) *errtrace.Error
  Exists(context.Context) (bool, *errtrace.Error)
  Delete(context.Context) (*errtrace.Error)
}

type Client interface{
  Topic(string) Topic
  Subscription(string) Subscription
  CreateTopic(context.Context, string) (Topic, *errtrace.Error)
  CreateSubscription(context.Context, string, googlePubsub.SubscriptionConfig) (Subscription, *errtrace.Error)
  Close() *errtrace.Error
}

type Message struct{
  Message *googlePubsub.Message
}

func (m *Message) Id() string {
  return m.Message.ID
}

func (m *Message) Payload() []byte {
  return m.Message.Data
}

func (m *Message) Ack() {
  m.Message.Ack()
}

func (m *Message) Nack() {
  m.Message.Nack()
}

type Integration struct {
  Client Client
  Topic Topic
  Subscription Subscription
}

func (i *Integration) Close() {
  i.Topic.Stop()
}

func (i *Integration) Publish(ctx context.Context, payload string) *googlePubsub.PublishResult {
  return i.Topic.Publish(ctx, &googlePubsub.Message{Data: []byte(payload)})
}

func (i *Integration) Receive(ctx context.Context, subscriptionName string, handler func(*Message)) *errtrace.Error {
  if i.Subscription == nil {
    subscription := i.Client.Subscription(subscriptionName)

    doesExist, err := subscription.Exists(ctx)

    if err != nil {
      return err
    }

    if doesExist == false {
      subscription, err = i.Client.CreateSubscription(ctx, subscriptionName, googlePubsub.SubscriptionConfig{
        Topic: i.Topic.WrappedTopic(),
        AckDeadline: 10 * time.Second,
      })

      if err != nil {
        return err
      }

      i.Subscription = subscription
    } else {
      i.Subscription = subscription
    }
  }

  err := i.Subscription.Receive(ctx, func(ctx context.Context, pubsubMessage *googlePubsub.Message){
    if pubsubMessage != nil {
      handler(&Message{Message: pubsubMessage})
    }
  })

  return err
}

func NewIntegration(ctx context.Context, client Client, topicName string) (*Integration, *errtrace.Error) {
  topic := client.Topic(topicName)

  ok, err := topic.Exists(ctx)

  if err != nil {
    return nil, err
  }

  if ok == false {
    return nil, errtrace.New(TopicDoesNotExist)
  }

  return &Integration{
    Client: client,
    Topic: topic,
  }, nil
}
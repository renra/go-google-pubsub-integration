package pubsubIntegration

import (
  "context"
  googlePubsub "cloud.google.com/go/pubsub"
  "github.com/renra/go-errtrace/errtrace"
)

type ClientWrapper struct {
  Client *googlePubsub.Client
}

func (w *ClientWrapper) CreateTopic(ctx context.Context, name string) (Topic, *errtrace.Error) {
  topic, err := w.Client.CreateTopic(ctx, name)

  if topic != nil {
    return &TopicWrapper{Topic: topic}, errtrace.Wrap(err)
  }

  return nil, errtrace.Wrap(err)
}

func (w *ClientWrapper) Topic(name string) (Topic) {
  topic := w.Client.Topic(name)
  return &TopicWrapper{Topic: topic}
}

func (w *ClientWrapper) CreateSubscription(ctx context.Context, name string, config googlePubsub.SubscriptionConfig) (Subscription, *errtrace.Error) {
  subscription, err := w.Client.CreateSubscription(ctx, name, config)

  wrappedSubscription := SubscriptionWrapper{Subscription: subscription}

  if err != nil {
    return &wrappedSubscription, errtrace.Wrap(err)
  } else {
    return &wrappedSubscription, nil
  }
}

func (w *ClientWrapper) Subscription(name string) Subscription {
  return &SubscriptionWrapper{Subscription: w.Client.Subscription(name)}
}

func (w *ClientWrapper) Close() *errtrace.Error {
  err := w.Client.Close()

  if err != nil {
    return errtrace.Wrap(err)
  } else {
    return nil
  }
}

func NewClient(ctx context.Context, projectName string) (*ClientWrapper, *errtrace.Error) {
  client, err := googlePubsub.NewClient(ctx, projectName)

  if err != nil {
    return nil, errtrace.Wrap(err)
  }

  return &ClientWrapper{Client: client}, nil
}


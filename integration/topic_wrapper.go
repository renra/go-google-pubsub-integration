package pubsubIntegration

import (
  "context"
  googlePubsub "cloud.google.com/go/pubsub"
  "github.com/renra/go-errtrace/errtrace"
)

type TopicWrapper struct {
  Topic *googlePubsub.Topic
}

func (t *TopicWrapper) WrappedTopic() *googlePubsub.Topic {
  return t.Topic
}

func (t *TopicWrapper) Publish(ctx context.Context, message *googlePubsub.Message) *googlePubsub.PublishResult {
  return t.Topic.Publish(ctx, message)
}

func (t *TopicWrapper) Exists(ctx context.Context) (bool, *errtrace.Error) {
  exists, err := t.Topic.Exists(ctx)
  return exists, errtrace.Wrap(err)
}

func (t *TopicWrapper) Delete(ctx context.Context) *errtrace.Error {
  return errtrace.Wrap(t.Topic.Delete(ctx))
}

func (t *TopicWrapper) Stop() {
  t.Topic.Stop()
}


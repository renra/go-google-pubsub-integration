package pubsubIntegration

import (
  "context"
  googlePubsub "cloud.google.com/go/pubsub"
  "github.com/renra/go-errtrace/errtrace"
)

type SubscriptionWrapper struct {
  Subscription *googlePubsub.Subscription
}

func (s *SubscriptionWrapper) Receive(ctx context.Context, handler func(context.Context, *googlePubsub.Message)) *errtrace.Error {
  return errtrace.Wrap(s.Subscription.Receive(ctx, handler))
}

func (s *SubscriptionWrapper) Exists(ctx context.Context) (bool, *errtrace.Error) {
  exists, err := s.Subscription.Exists(ctx)
  return exists, errtrace.Wrap(err)
}

func (s *SubscriptionWrapper) Delete(ctx context.Context) *errtrace.Error {
  return errtrace.Wrap(s.Subscription.Delete(ctx))
}

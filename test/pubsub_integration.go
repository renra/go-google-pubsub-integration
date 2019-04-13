package test

import (
  "time"
  "context"
  "github.com/stretchr/testify/mock"
  "github.com/stretchr/testify/suite"
  "github.com/stretchr/testify/assert"
  "app/integration"
  googlePubsub "cloud.google.com/go/pubsub"
  "github.com/renra/go-errtrace/errtrace"
)

type PubsubIntegrationSuite struct {
  suite.Suite
  projectName string
  topicName string
  subscriptionName string
  clientMock *ClientMock
}

type ClientMock struct {
  mock.Mock
}

func (c *ClientMock) CreateTopic(ctx context.Context, name string) (pubsubIntegration.Topic, *errtrace.Error) {
  args := c.Called(ctx, name)
  errorArg := args.Get(1)

  if errorArg != nil {
    return args.Get(0).(*TopicMock), args.Get(1).(*errtrace.Error)
  } else {
    return args.Get(0).(*TopicMock), nil
  }
}

func (c *ClientMock) Topic(name string) (pubsubIntegration.Topic) {
  args := c.Called(name)
  return args.Get(0).(*TopicMock)
}

func (c *ClientMock) CreateSubscription(ctx context.Context, name string, config googlePubsub.SubscriptionConfig) (pubsubIntegration.Subscription, *errtrace.Error) {
  args := c.Called(ctx, name, config)
  subscription := args.Get(0)
  errorArg := args.Get(1)

  if errorArg != nil {
    if subscription != nil {
      return args.Get(0).(*SubscriptionMock), args.Get(1).(*errtrace.Error)
    } else {
      return nil, args.Get(1).(*errtrace.Error)
    }
  } else {
    if subscription != nil {
      return args.Get(0).(*SubscriptionMock), nil
    } else {
      return nil, nil
    }
  }
}

func (c *ClientMock) Subscription(name string) (pubsubIntegration.Subscription) {
  args := c.Called(name)
  return args.Get(0).(*SubscriptionMock)
}

func (c *ClientMock) Close() *errtrace.Error {
  return nil
}

type TopicMock struct {
  mock.Mock
}

func (t *TopicMock) WrappedTopic() *googlePubsub.Topic {
  args := t.Called()
  return args.Get(0).(*googlePubsub.Topic)
}

func (t *TopicMock) Publish(ctx context.Context, m *googlePubsub.Message) *googlePubsub.PublishResult {
  args := t.Called(ctx, m)
  return args.Get(0).(*googlePubsub.PublishResult)
}

func (t *TopicMock) Exists(ctx context.Context) (bool, *errtrace.Error) {
  args := t.Called(ctx)
  errorArg := args.Get(1)

  if errorArg != nil {
    return args.Bool(0), args.Get(1).(*errtrace.Error)
  } else {
    return args.Bool(0), nil
  }
}

func (t *TopicMock) Delete(ctx context.Context) (*errtrace.Error) {
  args := t.Called(ctx)
  errorArg := args.Get(0)

  if errorArg != nil {
    return args.Get(0).(*errtrace.Error)
  } else {
    return nil
  }
}

func (t *TopicMock) Stop() {
  t.Called()
}

type SubscriptionMock struct {
  mock.Mock
}

var messageId string = "12"
var messagePayload string = "Message payload"
var messageEvent string = "MessageSent"
var messageAttributes map[string]string = map[string]string{ "event": messageEvent }

func (s *SubscriptionMock) Receive(ctx context.Context, handler func(context.Context, *googlePubsub.Message)) *errtrace.Error {
  handler(ctx, &googlePubsub.Message{
    ID: messageId,
    Data: []byte(messagePayload),
    Attributes: messageAttributes,
  })
  args := s.Called(ctx, handler)
  errorArg := args.Get(0)

  if errorArg != nil {
    return args.Get(0).(*errtrace.Error)
  } else {
    return nil
  }
}

func (s *SubscriptionMock) Exists(ctx context.Context) (bool, *errtrace.Error) {
  args := s.Called(ctx)
  errorArg := args.Get(1)

  if errorArg != nil {
    return args.Bool(0), args.Get(1).(*errtrace.Error)
  } else {
    return args.Bool(0), nil
  }
}

func (s *SubscriptionMock) Delete(ctx context.Context) (*errtrace.Error) {
  args := s.Called(ctx)
  errorArg := args.Get(0)

  if errorArg != nil {
    return args.Get(0).(*errtrace.Error)
  } else {
    return nil
  }
}

func (suite *PubsubIntegrationSuite) SetupSuite() {
  suite.projectName = "teams_test"
  suite.topicName = "teams_topic"
  suite.subscriptionName = "teams_subscription"
}

func (suite *PubsubIntegrationSuite) SetupTest() {
  suite.clientMock = &ClientMock{}
}

func (suite *PubsubIntegrationSuite) TestNewIntegration_TopicExists() {
  ctx := context.Background()

  topicMock := TopicMock{}

  topicMock.On("Exists", ctx).Return(true, nil).Once()
  suite.clientMock.On("Topic", suite.topicName).Return(&topicMock).Once()

  pubsub, err := pubsubIntegration.NewIntegration(ctx, suite.clientMock, suite.topicName)

  assert.Nil(suite.T(), err)
  assert.Equal(suite.T(), &topicMock, pubsub.Topic)
  assert.Equal(suite.T(), suite.clientMock, pubsub.Client)

  topicMock.AssertExpectations(suite.T())
  suite.clientMock.AssertExpectations(suite.T())
}

func (suite *PubsubIntegrationSuite) TestNewIntegration_TopicDoesNotExist() {
  ctx := context.Background()

  topicMock := TopicMock{}

  topicMock.On("Exists", ctx).Return(false, nil).Once()
  suite.clientMock.On("Topic", suite.topicName).Return(&topicMock).Once()

  pubsub, err := pubsubIntegration.NewIntegration(ctx, suite.clientMock, suite.topicName)

  assert.Nil(suite.T(), pubsub)
  assert.NotNil(suite.T(), err)
  assert.Equal(suite.T(), pubsubIntegration.TopicDoesNotExist, err.Error())

  topicMock.AssertExpectations(suite.T())
  suite.clientMock.AssertExpectations(suite.T())
}

func (suite *PubsubIntegrationSuite) TestNewIntegration_TopicRetrievalError() {
  ctx := context.Background()
  errorText := "Something went wrong"

  topicMock := TopicMock{}

  topicMock.On("Exists", ctx).Return(false, errtrace.New(errorText)).Once()
  suite.clientMock.On("Topic", suite.topicName).Return(&topicMock).Once()

  pubsub, err := pubsubIntegration.NewIntegration(ctx, suite.clientMock, suite.topicName)

  assert.Nil(suite.T(), pubsub)
  assert.NotNil(suite.T(), err)
  assert.Equal(suite.T(), errorText, err.Error())

  topicMock.AssertExpectations(suite.T())
  suite.clientMock.AssertExpectations(suite.T())
}

func (suite *PubsubIntegrationSuite) TestPublish() {
  ctx := context.Background()

  topicMock := TopicMock{}
  publishResult := googlePubsub.PublishResult{}
  payload := "Here's a message for you"
  attributes := map[string]string{"event":"something_happened"}

  topicMock.On("Exists", ctx).Return(true, nil).Once()
  topicMock.On(
    "Publish",
    ctx,
    mock.MatchedBy(func(message *googlePubsub.Message) bool {
      assert.Equal(suite.T(), payload, string(message.Data))
      assert.Equal(suite.T(), attributes, message.Attributes)

      return true
    }),
  ).Return(&publishResult).Once()

  suite.clientMock.On("Topic", suite.topicName).Return(&topicMock).Once()

  pubsub, err := pubsubIntegration.NewIntegration(ctx, suite.clientMock, suite.topicName)
  assert.Nil(suite.T(), err)

  result := pubsub.Publish(ctx, payload, attributes)

  assert.Equal(suite.T(), &publishResult, result)

  topicMock.AssertExpectations(suite.T())
  suite.clientMock.AssertExpectations(suite.T())
}

func (suite *PubsubIntegrationSuite) TestReceive_SubscriptionExists() {
  ctx := context.Background()
  handlerCalled := false

  topicMock := TopicMock{}
  subscriptionMock := SubscriptionMock{}

  topicMock.On("Exists", ctx).Return(true, nil).Once()

  subscriptionMock.On("Exists", ctx).Return(true, nil).Once()
  subscriptionMock.On(
    "Receive",
    ctx,
    mock.MatchedBy(func(handler func (ctx context.Context, message *googlePubsub.Message)) bool {
      return true
    }),
  ).Return(nil).Once()

  suite.clientMock.On("Topic", suite.topicName).Return(&topicMock).Once()
  suite.clientMock.On("Subscription", suite.subscriptionName).Return(&subscriptionMock).Once()

  pubsub, err := pubsubIntegration.NewIntegration(ctx, suite.clientMock, suite.topicName)
  assert.Nil(suite.T(), err)

  err = pubsub.Receive(ctx, suite.subscriptionName, func(message *pubsubIntegration.Message) {
    handlerCalled = true

    assert.NotNil(suite.T(), message)
    assert.Equal(suite.T(), messageId, string(message.Id()))
    assert.Equal(suite.T(), messagePayload, string(message.Payload()))
    assert.Equal(suite.T(), messageEvent, message.Event())
    assert.Equal(suite.T(), messageAttributes, message.Attributes())
  })

  assert.Nil(suite.T(), err)

  topicMock.AssertExpectations(suite.T())
  suite.clientMock.AssertExpectations(suite.T())

  assert.True(suite.T(), handlerCalled)
}

func (suite *PubsubIntegrationSuite) TestReceive_SubscriptionDoesNotExist() {
  ctx := context.Background()
  handlerCalled := false
  handlerCalledSecondTime := false

  topicMock := TopicMock{}
  subscriptionMock := SubscriptionMock{}
  createdSubscriptionMock := SubscriptionMock{}
  googlePubsubTopic := googlePubsub.Topic{}

  topicMock.On("Exists", ctx).Return(true, nil).Once()
  topicMock.On("WrappedTopic").Return(&googlePubsubTopic).Once()

  subscriptionMock.On("Exists", ctx).Return(false, nil).Once()

  suite.clientMock.On("Topic", suite.topicName).Return(&topicMock).Once()
  suite.clientMock.On("Subscription", suite.subscriptionName).Return(&subscriptionMock).Once()

  suite.clientMock.On(
    "CreateSubscription",
    ctx,
    suite.subscriptionName,
    googlePubsub.SubscriptionConfig{
      Topic: &googlePubsubTopic,
      AckDeadline: 10 * time.Second,
    },
  ).Return(&createdSubscriptionMock, nil).Once()

  createdSubscriptionMock.On(
    "Receive",
    ctx,
    mock.MatchedBy(func(handler func (ctx context.Context, message *googlePubsub.Message)) bool {
      return true
    }),
  ).Return(nil).Twice()

  pubsub, err := pubsubIntegration.NewIntegration(ctx, suite.clientMock, suite.topicName)
  assert.Nil(suite.T(), err)

  err = pubsub.Receive(ctx, suite.subscriptionName, func(message *pubsubIntegration.Message) {
    handlerCalled = true

    assert.NotNil(suite.T(), message)
    assert.Equal(suite.T(), messageId, string(message.Id()))
    assert.Equal(suite.T(), messagePayload, string(message.Payload()))
    assert.Equal(suite.T(), messageEvent, message.Event())
    assert.Equal(suite.T(), messageAttributes, message.Attributes())
  })

  assert.Nil(suite.T(), err)

  err = pubsub.Receive(ctx, suite.subscriptionName, func(message *pubsubIntegration.Message) {
    handlerCalledSecondTime = true

    assert.NotNil(suite.T(), message)
    assert.Equal(suite.T(), messageId, string(message.Id()))
    assert.Equal(suite.T(), messagePayload, string(message.Payload()))
    assert.Equal(suite.T(), messageEvent, message.Event())
    assert.Equal(suite.T(), messageAttributes, message.Attributes())
  })

  assert.Nil(suite.T(), err)

  topicMock.AssertExpectations(suite.T())
  suite.clientMock.AssertExpectations(suite.T())

  assert.True(suite.T(), handlerCalled)
  assert.True(suite.T(), handlerCalledSecondTime)
}

func (suite *PubsubIntegrationSuite) TestReceive_SubscriptionDoesNotExist_FailedToCreate() {
  ctx := context.Background()
  handlerCalled := false

  topicMock := TopicMock{}
  subscriptionMock := SubscriptionMock{}
  googlePubsubTopic := googlePubsub.Topic{}
  creationError := errtrace.New("It got broken")

  topicMock.On("Exists", ctx).Return(true, nil).Once()
  topicMock.On("WrappedTopic").Return(&googlePubsubTopic).Once()

  subscriptionMock.On("Exists", ctx).Return(false, nil).Once()

  suite.clientMock.On("Topic", suite.topicName).Return(&topicMock).Once()
  suite.clientMock.On("Subscription", suite.subscriptionName).Return(&subscriptionMock).Once()

  suite.clientMock.On(
    "CreateSubscription",
    ctx,
    suite.subscriptionName,
    googlePubsub.SubscriptionConfig{
      Topic: &googlePubsubTopic,
      AckDeadline: 10 * time.Second,
    },
  ).Return(nil, creationError).Once()

  pubsub, err := pubsubIntegration.NewIntegration(ctx, suite.clientMock, suite.topicName)
  assert.Nil(suite.T(), err)

  err = pubsub.Receive(ctx, suite.subscriptionName, func(message *pubsubIntegration.Message) {
    handlerCalled = true

    assert.NotNil(suite.T(), message)
    assert.Equal(suite.T(), messageId, string(message.Id()))
    assert.Equal(suite.T(), messagePayload, string(message.Payload()))
    assert.Equal(suite.T(), messageEvent, message.Event())
    assert.Equal(suite.T(), messageAttributes, message.Attributes())
  })

  assert.Equal(suite.T(), creationError.Error(), err.Error())

  topicMock.AssertExpectations(suite.T())
  suite.clientMock.AssertExpectations(suite.T())

  assert.False(suite.T(), handlerCalled)
}


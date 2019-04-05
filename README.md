# Google PubSub Integration

A thin decorator to make my life with Google PubSub a little easier.

### Usage

```go
package myPackage

import (
  "context"
  "github.com/renra/go-google-pubsub-integration/pubsubIntegration"
)

func main() {
  ctx := context.Background()

  projectName := "SaveTheWorld"
  topcName := "brilliant_ideas"
  subscriptionName := "subscription_for_brilliant_ideas"

  client, err := pubsubIntegration.NewClient(ctx, projectName)

  if err != nil {
    panic(err)
  }

  pubsub, err := pubsubIntegration.NewIntegration(ctx, client, topicName)

  if err != nil {
    panic(err)
  }

  pubsub.Publish(ctx, "This is my message")

  pubsub.Receive(ctx, subscriptionName, func(message *pubsubIntegration.Message){
    fmt.Println(fmt.Sprintf("I've just received a message: %s", message.Id()))
    fmt.Println(fmt.Sprintf("It says: %s", message.Payload()))
  })
}
```

When calling `NewIntegration`, it checks whether the topic exists. It returns an error if it doesn't. You can call `Client.CreateTopic(context.Context, string) Topic, *errtrace.Error` to create one. The reason is that you need the topic for both `Publish`ing and `Receive`ing so an integration without a topic does not make much sense.

When calling `Receive` you pass in a subscription name. The integration checks whether the subscription exists and tries to create it if it doesn't. In case of failure, an error is returned. You can only receive messages with a working subscription.

It uses [errtrace](https://github.com/renra/go-errtrace) to return errors with stack traces for easier debugging.

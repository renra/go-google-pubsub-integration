package pubsubIntegration

import (
  googlePubsub "cloud.google.com/go/pubsub"
)

type Message struct{
  Message *googlePubsub.Message
}

func NewMessage(id string, payload string, attributes map[string]string) *Message {
  return &Message {
    Message: &googlePubsub.Message {
      ID: id,
      Data: []byte(payload),
      Attributes: attributes,
    },
  }
}

func (m *Message) Id() string {
  return m.Message.ID
}

func (m *Message) Payload() []byte {
  return m.Message.Data
}

func (m *Message) Attributes() map[string]string {
  return m.Message.Attributes
}

func (m *Message) Event() string {
  return m.Message.Attributes["event"]
}

func (m *Message) Ack() {
  m.Message.Ack()
}

func (m *Message) Nack() {
  m.Message.Nack()
}

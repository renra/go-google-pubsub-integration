package test

import (
  "testing"
  "github.com/stretchr/testify/suite"
)

func TestPubsubIntegration(t *testing.T) {
  suite.Run(t, new(PubsubIntegrationSuite))
}

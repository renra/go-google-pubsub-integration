SOURCES=./

dep:
	dep ensure

.PHONY: test
.DEFAULT_GOAL := test
test:
	go test ./test... -count 1 -v

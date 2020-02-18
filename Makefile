lint:
	gofmt -w .
	golint ./...

test: lint
	mkdir var || true
	go test -cover -coverprofile var/coverage.out ./... -count=1
	go tool cover -html=var/coverage.out -o var/coverage.html

update-deps:
	go get -u
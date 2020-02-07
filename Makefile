test:
	gofmt -s -w *.go
	go test

update-deps:
	go get -u
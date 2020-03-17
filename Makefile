DC=docker-compose
DE=docker-compose exec -T app

.env:
	sed -e 's/{DEV_UID}/$(shell id -u)/g' \
		-e 's/{DEV_GID}/$(shell id -g)/g' \
		-e 's|{GITLAB_CI}|$(shell [ ! -z "$$GITLAB_CI" ] && echo true || echo false)|g' \
		-e 's|{DOCKER_SOCKET_PATH}|$(shell test -S /var/run/docker-$${USER}.sock && echo /var/run/docker-$${USER}.sock || echo /var/run/docker.sock)|g' \
		.env.dist >> .env; \


docker-up-force: .env
	$(DC) pull
	$(DC) up -d --force-recreate --remove-orphans

docker-down-clean: .env
	$(DC) down -v

docker-compose.ci.yml:
	# Comment out any port forwarding
	sed -r 's/^(\s+ports:)$$/#\1/g; s/^(\s+- \$$\{DEV_IP\}.*)$$/#\1/g; s/^(\s+- \$$\{GOPATH\}.*)$$/#\1/g' docker-compose.yml > docker-compose.ci.yml

go-update:
	$(DE) su-exec root go get -u all
	$(DE) su-exec root go mod tidy

init-dev: docker-up-force wait-for-server-start
	$(DE) go mod download

lint:
	$(DE) gofmt -w .
	$(DE) golint ./...

wait-for-server-start:
	$(DE) /bin/sh -c 'while [ $$(curl -s -o /dev/null -w "%{http_code}" http://guest:guest@rabbitmq:15672/api/overview) == 000 ]; do sleep 1; done'

fast-test: lint
	$(DE) mkdir var || true
	$(DE) go test -cover -coverprofile var/coverage.out ./... -count=1
	$(DE) go tool cover -html=var/coverage.out -o var/coverage.html

test: init-dev fast-test
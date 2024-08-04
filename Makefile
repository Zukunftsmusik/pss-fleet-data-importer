.PHONY: all
all: format check test

# setup
.PHONY: init-dev
init-dev:
	rye self update
	rye sync --update-all
	pre-commit install
	pre-commit run --all-files

.PHONY: update
update:
	rye sync --update-all

# formatting and linting
.PHONY: check
check:
	flake8 ./src
	vulture

.PHONY: format
format:
	autoflake .
	isort .
	black .

# testing
.PHONY: coverage
coverage:
	pytest --cov=./src/importer --cov-report=xml:cov.xml

.PHONY: test
test:
	pytest

# run
.PHONY: docker
docker:
	-docker stop container-pss-fleet-data-importer
	docker rm -f container-pss-fleet-data-importer
	docker image rm -f image-pss-fleet-data-importer:latest
	docker build -t image-pss-fleet-data-importer .
	docker run -d --name container-pss-fleet-data-importer --env-file ./.docker-env image-pss-fleet-data-importer:latest

.PHONY: run
run:
	python main.py

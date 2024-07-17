.PHONY: all
all: format check test

.PHONY: test
test:
	rye run pytest

.PHONY: format
format:
	rye run autoflake .
	rye run isort .
	rye run black .

.PHONY: check
check:
	rye run flake8 ./src
	rye run vulture

.PHONY: update
update:
	rye sync --update-all

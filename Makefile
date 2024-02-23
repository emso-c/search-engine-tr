# OS_NAME := $(shell uname -s)

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

hello: ## Say hello
	@echo "Hello, World!"

clear-pycache: ## Clear pycache except .env folder
	@find . -name "__pycache__" -type d -not -path "./.env/*" -exec rm -rf {} \ > /dev/null 2>&1 || true
	@echo "Pycache cleared"

run: ## Run the application
	@sh ./bin/run.sh

run-scanner: ## Run the scanner
	@sh ./bin/run_scanner.sh
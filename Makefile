DOCKER_PLATFORM=linux/arm64

SERVICE_NAME="Clazar exporter"
ENVIRONMENT=DEV
VENV_DIR=venv

# Load variables from .env if it exists
ifneq (,$(wildcard .env))
    include .env
    export $(shell sed 's/=.*//' .env)
endif

default: release

# Development
.PHONY: venv
venv:
	@echo "Creating virtual environment..."
	python3 -m venv $(VENV_DIR)
	@echo "Virtual environment created at $(VENV_DIR)"
	@echo "To activate, run: source $(VENV_DIR)/bin/activate"

.PHONY: build
build: venv
	@echo "Installing dependencies..."
	$(VENV_DIR)/bin/pip install --upgrade pip
	$(VENV_DIR)/bin/pip install -r requirements.txt
	@echo "Build complete! Virtual environment ready at $(VENV_DIR)"

.PHONY: clean
clean:
	@echo "Cleaning up..."
	rm -rf $(VENV_DIR)
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Cleanup complete!"

# Omnistrate
.PHONY: install-ctl
install-ctl:
	@brew install omnistrate/tap/omnistrate-ctl

.PHONY: upgrade-ctl
upgrade-ctl:
	@brew upgrade omnistrate/tap/omnistrate-ctl
	
.PHONY: login
login:
	cat ./.omnistrate.password | omnistrate-ctl login --email $(OMNISTRATE_EMAIL) --password-stdin

.PHONY: release
release:
	sed -i '' "s#\$${IMAGE_VERSION}#$$IMAGE_VERSION#g" omnistrate-compose.yaml
	@echo "Releasing service plan to Omnistrate" 
	@omnistrate-ctl build -f omnistrate-compose.yaml --product-name ${SERVICE_NAME}  --environment ${ENVIRONMENT} --environment-type ${ENVIRONMENT}  --release-as-preferred

.PHONY: docker-build
docker-build:
	docker buildx build --platform=${DOCKER_PLATFORM} -f ./Dockerfile -t usage-export-clazar-recipe:latest . 

.PHONY: docker-run
docker-run: docker-build
	docker run -p 8080:8080 usage-export-clazar-recipe:latest

.PHONY: run
run: build
	@echo "Running $(SERVICE_NAME)..."
	$(VENV_DIR)/bin/python src/main.py

.PHONY: unit-tests
unit-tests: build
	@echo "Running unit tests..."
	$(VENV_DIR)/bin/python -m unittest discover -s tests -p "test_*.py" -v

.PHONY: integration-tests
integration-tests: build
	@echo "Running integration tests..."
	$(VENV_DIR)/bin/python -m unittest discover -s integration_tests -p "test_*.py" -v

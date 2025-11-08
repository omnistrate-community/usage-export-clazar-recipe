DOCKER_PLATFORM=linux/arm64

SERVICE_NAME="Clazar exporter"
ENVIRONMENT=DEV

# Load variables from .env if it exists
ifneq (,$(wildcard .env))
    include .env
    export $(shell sed 's/=.*//' .env)
endif

default: release

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
	docker buildx build --platform=${DOCKER_PLATFORM} -f ./Dockerfile -t usage-export-clazar:latest . 

.PHONY: docker-run
docker-run: docker-build
	docker run -p 8080:8080 usage-export-clazar:latest

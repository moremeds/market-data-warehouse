DOCKER_USER ?= moremeds
IMAGE_NAME  ?= ibroker-mkt-data
TAG         ?= latest
FULL_IMAGE   = $(DOCKER_USER)/$(IMAGE_NAME):$(TAG)
DOCKERFILE   = docker/ibroker-mkt-data/Dockerfile
PLATFORMS    = linux/amd64,linux/arm64
BUILDER      = mdw-multiarch

.PHONY: build push run seed stop logs clean builder

# Build multi-platform and push to Docker Hub
push: builder
	docker buildx build \
		--platform $(PLATFORMS) \
		-f $(DOCKERFILE) \
		-t $(FULL_IMAGE) \
		--push .

# Build locally (current platform only)
build:
	docker compose build ibroker-mkt-data

# Create multi-platform builder (one-time)
builder:
	@docker buildx inspect $(BUILDER) > /dev/null 2>&1 || \
		docker buildx create --name $(BUILDER) --use
	@docker buildx use $(BUILDER)

# Run once immediately
run:
	docker compose run --rm ibroker-mkt-data --now

# Seed initial data
seed:
	docker compose run --rm ibroker-mkt-data --seed --preset presets/sp500.json

# Start scheduler (background)
up:
	docker compose up -d ibroker-mkt-data

# Stop everything
stop:
	docker compose down --remove-orphans

# View logs
logs:
	docker compose logs -f ibroker-mkt-data

# Clean up orphan containers and images
clean:
	docker compose down --remove-orphans
	docker image prune -f

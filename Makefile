# Makefile for Integrator Core

# Configuration
BASE_IMAGE_REPO ?= flowstash/flowstash-base
BASE_IMAGE_TAG ?= latest
MANAGED_API_IMAGE ?= us-east1-docker.pkg.dev/easypie-prod-411815/flowstash-container-repo/flowstash-api

.PHONY: build-base publish-base deploy-managed

# Build the base docker image for x86 architecture (linux/amd64)
# This is necessary when building on ARM-based Macs (M1/M2/M3/M4) for x86 targets
build-base:
	docker build --platform linux/amd64 -t flowstash-base:$(BASE_IMAGE_TAG) -f deployment/docker/base.Dockerfile .

# Publish the base image to a public repository
publish-base: build-base
	docker tag flowstash-base:$(BASE_IMAGE_TAG) $(BASE_IMAGE_REPO):$(BASE_IMAGE_TAG)
	docker push $(BASE_IMAGE_REPO):$(BASE_IMAGE_TAG)

# Deploy the managed service API to Google Artifact Registry
deploy-managed:
	docker build --platform linux/amd64 -t managed-api -f managed/Dockerfile .
	docker tag managed-api $(MANAGED_API_IMAGE)
	docker push $(MANAGED_API_IMAGE)
	kubectl rollout restart deployment flowstash-api --context=ep-prod
# kubectl apply -f managed/deployment/k8s/api-deployment.yaml --context=ep-prod 

.PHONY: install test lint format build release-check publish clean bump-version

install:
	poetry install

test:
	poetry run pytest

lint:
	poetry run ruff check .

format:
	poetry run ruff format .

build:
	cd packages/flowstash_clients && POETRY_VIRTUALENVS_CREATE=false poetry build
	cd packages/flowstash_lib && POETRY_VIRTUALENVS_CREATE=false poetry build
	cd packages/flowstash_runtime && POETRY_VIRTUALENVS_CREATE=false poetry build
	cd packages/flowstash-cli && POETRY_VIRTUALENVS_CREATE=false poetry build
	cd packages/flowstash && POETRY_VIRTUALENVS_CREATE=false poetry build

release-check: test build
	@echo "Running release preflight checks..."
	@echo "Release check complete."

publish: 
	cd packages/flowstash_clients && POETRY_VIRTUALENVS_CREATE=false poetry publish
	cd packages/flowstash_lib && POETRY_VIRTUALENVS_CREATE=false poetry publish
	cd packages/flowstash_runtime && POETRY_VIRTUALENVS_CREATE=false poetry publish
	cd packages/flowstash-cli && POETRY_VIRTUALENVS_CREATE=false poetry publish
	cd packages/flowstash && POETRY_VIRTUALENVS_CREATE=false poetry publish

clean:
	rm -rf packages/*/.venv
	rm -rf packages/*/dist

bump-version:
	poetry version minor
	@$(MAKE) update-internal-versions

bump-version-patch:
	poetry version patch
	@$(MAKE) update-internal-versions

update-internal-versions:
	@NEW_VERSION=$$(poetry version -s); \
	NEXT_MINOR=$$(echo $$NEW_VERSION | awk -F. '{print $$1"."$$2+1".0"}'); \
	echo "Bumping all packages to $$NEW_VERSION (upper bound $$NEXT_MINOR)..."; \
	for dir in packages/*; do \
		if [ -d "$$dir" ] && [ -f "$$dir/pyproject.toml" ]; then \
			(cd "$$dir" && \
			 poetry version $$NEW_VERSION && \
			 python3 -c "import sys, re; new_v = '$$NEW_VERSION'; next_v = '$$NEXT_MINOR'; f = open('pyproject.toml', 'r+'); content = f.read(); new_content = re.sub(r'\"(flowstash[a-z-]*?)>=[^,\"]+(?:,<[^,\"]+)?\"', rf'\"\1>={new_v},<{next_v}\"', content); f.seek(0); f.write(new_content); f.truncate(); f.close()"); \
		fi; \
	done
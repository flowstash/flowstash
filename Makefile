# Makefile for Integrator Core

# Configuration
BASE_IMAGE_REPO ?= flowstash/flowstash-base
BASE_IMAGE_TAG ?= latest
MANAGED_API_IMAGE ?= flowstash/flowstash-base
export PATH := $(CURDIR)/.venv/bin:$(PATH)
.PHONY: build-base publish-base

# Build the base docker image for x86 architecture (linux/amd64)
# This is necessary when building on ARM-based Macs (M1/M2/M3/M4) for x86 targets
build-base:
	docker build --platform linux/amd64 -t flowstash-base:$(BASE_IMAGE_TAG) -f deployment/docker/base.Dockerfile .

# Publish the base image to a public repository
publish-base: build-base
	docker tag flowstash-base:$(BASE_IMAGE_TAG) $(BASE_IMAGE_REPO):$(BASE_IMAGE_TAG)
	docker push $(BASE_IMAGE_REPO):$(BASE_IMAGE_TAG)



.PHONY: test lint format build release-check publish publish-rc clean bump-version bump-version-patch bump-version-rc finalize-rc-version new-version new-patch new-rc-version


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

bump-version-rc:
	@CURRENT_VERSION=$$(poetry version -s); \
	if echo "$$CURRENT_VERSION" | grep -Eq 'rc[0-9]+$$'; then \
		poetry version prerelease; \
	elif echo "$$CURRENT_VERSION" | grep -Eq 'b[0-9]+$$'; then \
		poetry version prerelease --next-phase; \
	elif echo "$$CURRENT_VERSION" | grep -Eq 'a[0-9]+$$'; then \
		poetry version prerelease --next-phase; \
		poetry version prerelease --next-phase; \
	else \
		poetry version prerelease; \
		poetry version prerelease --next-phase; \
		poetry version prerelease --next-phase; \
	fi
	@$(MAKE) update-internal-versions

finalize-rc-version:
	@CURRENT_VERSION=$$(poetry version -s); \
	if echo "$$CURRENT_VERSION" | grep -Eq 'rc[0-9]+$$'; then \
		poetry version prerelease --next-phase; \
	else \
		echo "Current version '$$CURRENT_VERSION' is not an rc version"; \
		exit 1; \
	fi
	@$(MAKE) update-internal-versions

new-version: bump-version build build-base publish

new-patch: bump-version-patch build build-base publish

new-rc-version: bump-version-rc build publish
	@NEW_VERSION=$$(poetry version -s); \
	echo "Done... you can install it like this"; \
	echo "uv add \"flowstash>=$$NEW_VERSION\" --prerelease allow"

publish-rc: finalize-rc-version build publish

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
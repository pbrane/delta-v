# ==============================================================================
# Delta-V Build Facade
#
# Usage:
#   make build                          Compile and install all 21 modules (tests skipped)
#   make test                           Build and run all tests
#   make test-class MODULE=:org.opennms.core.daemon-boot-pollerd TEST=SomeDaoTest
#   make daemon DAEMON=provisiond       Rebuild a single daemon boot JAR
#   make clean                          Remove all build artifacts
#
# Runtime / lifecycle:
#   make images                         Build all Docker images (daemons + auxiliaries)
#   make daemon-image DAEMON=provisiond Build a single daemon Docker image
#   make up PROFILE=full                Start the stack (profiles: active|passive|full|demo)
#   make down | status | logs SVC=x     Stop / status / tail logs
#   make verify                         Run deploy health checks
#   make dev                            Build all images, then bring the stack up
#   make doctor                         Preflight: JDK 21, Docker, GH Packages auth, images
#
# Overridable variables:
#   MODULE           Maven module selector (e.g. :org.opennms.core.daemon-boot-pollerd)
#   DAEMON           Daemon short name for single-daemon target (e.g. provisiond, minion)
#   TEST             Test class name (suffix IT = integration test)
#   MAVEN_FLAGS      Extra Maven flags (default: -DskipTests -B)
#   MAVEN_OPTS       JVM options for Maven
# ==============================================================================

MODULE       ?=
DAEMON       ?=
TEST         ?=
PROFILE      ?=
SVC          ?=
MAVEN_FLAGS  ?= -DskipTests -B
PUSH         ?= false
PLATFORMS    ?=
IMAGE_PREFIX ?=
MAVEN_OPTS  ?= -Xmx3g \
               -XX:ReservedCodeCacheSize=512m \
               -XX:+TieredCompilation \
               -XX:TieredStopAtLevel=1 \
               -XX:-UseGCOverheadLimit \
               -XX:+UseParallelGC \
               -XX:-MaxFDLimit \
               -Djdk.util.zip.disableZip64ExtraFieldValidation=true \
               -Dmaven.wagon.http.retryHandler.count=3

MVN    := ./mvnw
DELTAV := deploy

export MAVEN_OPTS

.PHONY: help build test test-class daemon clean images daemon-image up down reset status logs verify dev doctor c4-edit c4-export

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Variables (override on command line):"
	@echo "  MODULE           Maven module selector                              (current: $(MODULE))"
	@echo "  DAEMON           Daemon short name (e.g. provisiond)                (current: $(DAEMON))"
	@echo "  TEST             Test class name (suffix IT = integration test)     (current: $(TEST))"
	@echo "  MAVEN_FLAGS      Extra Maven flags                                  (current: $(MAVEN_FLAGS))"
	@echo "  PROFILE          Compose profile for 'up' (active|passive|full|demo) (current: $(PROFILE))"
	@echo "  SVC              Service name for 'logs'                            (current: $(SVC))"

build: ## Compile and install all modules (tests skipped)
	$(MVN) $(MAVEN_FLAGS) install

test: ## Build and run all tests
	$(MVN) -B verify

test-class: ## Run a single test class; set MODULE and TEST
	@test -n "$(MODULE)" || (echo "ERROR: MODULE is required" && exit 1)
	@test -n "$(TEST)"   || (echo "ERROR: TEST is required" && exit 1)
	$(MVN) -B \
	  --projects $(MODULE) \
	  --also-make \
	  $(if $(filter %IT,$(TEST)),-Dit.test=$(TEST),-Dtest=$(TEST) -DskipTests=false) \
	  $(if $(filter %IT,$(TEST)),failsafe:integration-test failsafe:verify,install)

daemon: ## Rebuild a single daemon boot JAR; set DAEMON=provisiond (etc)
	@test -n "$(DAEMON)" || (echo "ERROR: DAEMON is required, e.g.: make daemon DAEMON=provisiond" && exit 1)
	$(MVN) -B -DskipTests \
	  --projects :org.opennms.core.daemon-boot-$(DAEMON) \
	  --also-make \
	  install

clean: ## Remove all build artifacts
	$(MVN) -B clean

images: ## Build ALL Docker images (daemons + auxiliaries)
	cd $(DELTAV) && PUSH=$(PUSH) PLATFORMS=$(PLATFORMS) IMAGE_PREFIX=$(IMAGE_PREFIX) ./build.sh deltav

daemon-image: ## Build one daemon image (DAEMON=); reuses cached base
	@test -n "$(DAEMON)" || (echo "ERROR: DAEMON is required, e.g.: make daemon-image DAEMON=alarmd" && exit 1)
	cd $(DELTAV) && ./build.sh daemon $(DAEMON)

up: ## Start the stack (PROFILE=active|passive|full|demo)
	cd $(DELTAV) && ./deploy.sh up $(PROFILE)

down: ## Stop the stack (preserve data)
	cd $(DELTAV) && ./deploy.sh down

reset: ## Stop and remove all data volumes
	cd $(DELTAV) && ./deploy.sh reset

status: ## Show service status
	cd $(DELTAV) && ./deploy.sh status

logs: ## Tail logs (SVC=<service>)
	cd $(DELTAV) && ./deploy.sh logs $(SVC)

verify: ## Run deploy health checks
	cd $(DELTAV) && ./deploy.sh test

dev: ## Build all images, then bring the stack up (sequential; safe under make -j)
	cd $(DELTAV) && PUSH=$(PUSH) PLATFORMS=$(PLATFORMS) IMAGE_PREFIX=$(IMAGE_PREFIX) ./build.sh deltav
	cd $(DELTAV) && ./deploy.sh up $(PROFILE)

doctor: ## Preflight: verify the environment can build & run
	cd $(DELTAV) && ./doctor.sh

# --- C4 architecture diagrams ---
C4_DIR  := docs/architecture/c4
C4_PORT ?= 8080
# structurizr/lite:latest is now a deprecation stub that exits immediately;
# 2025.11.08 is the last release that actually serves the Lite web app.
C4_LITE_IMAGE := structurizr/lite:2025.11.08

c4-edit: ## Serve Structurizr Lite for live editing (C4_PORT, default 8080)
	docker run -it --rm -p $(C4_PORT):8080 -v "$(PWD)/$(C4_DIR)":/usr/local/structurizr $(C4_LITE_IMAGE)

c4-export: ## Render all C4 views to SVG + PNG in $(C4_DIR)/exports
	@echo "Starting Structurizr Lite..."
	@docker run -d --rm --name deltav-c4-lite -p $(C4_PORT):8080 \
		-v "$(PWD)/$(C4_DIR)":/usr/local/structurizr $(C4_LITE_IMAGE)
	@echo "Waiting for Lite to come up..."
	@until curl -sf http://localhost:$(C4_PORT)/workspace/diagrams >/dev/null 2>&1; do sleep 2; done
	@cd $(C4_DIR)/scripts && npm install --silent && \
		node export-diagrams.js http://localhost:$(C4_PORT)/workspace/diagrams both ../exports
	@docker stop deltav-c4-lite >/dev/null
	@echo "Exports written to $(C4_DIR)/exports/{svg,png}"

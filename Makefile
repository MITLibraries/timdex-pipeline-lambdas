SHELL=/bin/bash
DATETIME:=$(shell date -u +%Y%m%dT%H%M%SZ)
ECR_REGISTRY_DEV=$(shell aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com

help: ## Print this message
	@awk 'BEGIN { FS = ":.*##"; print "Usage:  make <target>\n\nTargets:" } \
/^[-_[:alpha:]]+:.?*##/ { printf "  %-15s%s\n", $$1, $$2 }' $(MAKEFILE_LIST)

dist-dev: ## Build docker container
	docker build --platform linux/amd64 -t $(ECR_REGISTRY_DEV)/timdex-input-format-dev:latest \
		-t $(ECR_REGISTRY_DEV)/timdex-input-format-dev:`git describe --always` .	

publish-dev: dist ## Build, tag and push
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_REGISTRY_DEV)
	docker push $(ECR_REGISTRY_DEV)/timdex-input-format-dev:latest
	docker push $(ECR_REGISTRY_DEV)/timdex-input-format-dev:`git describe --always`

update-format-lambda-dev: ## Updates the lambda with whatever is the most recent image in the ecr
	aws lambda update-function-code \
		--function-name timdex-format-dev \
		--image-uri $(shell aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com/timdex-input-format-dev:latest


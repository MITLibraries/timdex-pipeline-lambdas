### This is the Terraform-generated header for the timdex-pipeline-lambads Makefile ###
SHELL=/bin/bash
DATETIME:=$(shell date -u +%Y%m%dT%H%M%SZ)
### This is the Terraform-generated header for timdex-pipeline-lambdas-dev
ECR_NAME_DEV:=timdex-pipeline-lambdas-dev
ECR_URL_DEV:=222053980223.dkr.ecr.us-east-1.amazonaws.com/timdex-pipeline-lambdas-dev
FUNCTION_DEV:=timdex-format-dev
### End of Terraform-generated header ###

help: ## Print this message
	@awk 'BEGIN { FS = ":.*##"; print "Usage:  make <target>\n\nTargets:" } \
/^[-_[:alpha:]]+:.?*##/ { printf "  %-15s%s\n", $$1, $$2 }' $(MAKEFILE_LIST)

### Developer Deploy Commands ###
dist-dev: ## Build docker container (intended for developer-based manual build)
	docker build --platform linux/amd64 \
	    -t $(ECR_URL_DEV):latest \
		-t $(ECR_URL_DEV):`git describe --always` \
		-t $(ECR_NAME_DEV):latest .

publish-dev: dist-dev ## Build, tag and push (intended for developer-based manual publish)
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_URL_DEV)
	docker push $(ECR_URL_DEV):latest
	docker push $(ECR_URL_DEV):`git describe --always`

update-lambda-dev: ## Updates the lambda with whatever is the most recent image in the ecr (intended for developer-based manual update)
	aws lambda update-function-code \
		--function-name $(FUNCTION_DEV) \
		--image-uri $(ECR_URL_DEV):latest
		
### This is the Terraform-generated header for the timdex-pipeline-lambads Makefile ###
SHELL=/bin/bash
DATETIME:=$(shell date -u +%Y%m%dT%H%M%SZ)
ECR:=222053980223.dkr.ecr.us-east-1.amazonaws.com
ECR_NAME:=timdex-pipeline-lambdas-dev
ECR_URL:=222053980223.dkr.ecr.us-east-1.amazonaws.com/timdex-pipeline-lambdas-dev
FUNCTION:=timdex-format-dev
### End of Terraform-generated header ###


help: ## Print this message
	@awk 'BEGIN { FS = ":.*##"; print "Usage:  make <target>\n\nTargets:" } \
/^[-_[:alpha:]]+:.?*##/ { printf "  %-15s%s\n", $$1, $$2 }' $(MAKEFILE_LIST)

dist-dev: ## Build docker container
	docker build --platform linux/amd64 \
	    -t $(ECR_URL):latest \
		-t $(ECR_URL):`git describe --always` \
		-t $(ECR_NAME):latest .

publish-dev: dist-dev ## Build, tag and push
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_URL)
	docker push $(ECR_URL):latest
	docker push $(ECR_URL):`git describe --always`

update-lambda-dev: ## Updates the lambda with whatever is the most recent image in the ecr
	aws lambda update-function-code --function-name $(FUNCTION) --image-uri $(ECR_URL):latest

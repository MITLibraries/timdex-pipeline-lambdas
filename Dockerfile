FROM public.ecr.aws/lambda/python:3.12

# Copy function code
COPY . ${LAMBDA_TASK_ROOT}/

# Install git (required for installation of timdex-dataset-api)
RUN dnf install -y git

# Install dependencies
RUN pip3 install pipenv
RUN pipenv requirements > requirements.txt
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Default handler. See README for how to override to a different handler.
CMD [ "lambdas.format_input.lambda_handler" ]

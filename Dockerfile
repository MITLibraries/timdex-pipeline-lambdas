FROM public.ecr.aws/lambda/python:3.13

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN dnf install -y git && dnf clean all

COPY pyproject.toml uv.lock* ./

RUN cd ${LAMBDA_TASK_ROOT} && \
    uv export --format requirements-txt --no-hashes --no-dev > requirements.txt && \
    uv pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}" --system

COPY . ${LAMBDA_TASK_ROOT}/

CMD ["lambdas.format_input.lambda_handler"]

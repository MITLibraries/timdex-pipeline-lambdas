FROM public.ecr.aws/lambda/python:3.9

# Copy function code
COPY format.py ${LAMBDA_TASK_ROOT}
COPY ping.py ${LAMBDA_TASK_ROOT}

# Default handler. See README for how to override to a different handler.
CMD [ "format.lambda_handler" ]

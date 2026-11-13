FROM public.ecr.aws/lambda/python:3.12
COPY backend/requirements-lambda.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ${LAMBDA_TASK_ROOT}/common/
COPY backend/ ${LAMBDA_TASK_ROOT}/backend/
CMD ["backend.handler.handler"]

FROM public.ecr.aws/lambda/python:3.12
COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ${LAMBDA_TASK_ROOT}/common/
COPY consumer/handler.py ${LAMBDA_TASK_ROOT}/handler.py
CMD ["handler.handler"]

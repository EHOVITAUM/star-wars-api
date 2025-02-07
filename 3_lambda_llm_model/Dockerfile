FROM public.ecr.aws/lambda/python:3.12
COPY lambda_file.py ./
COPY requirements.txt ./
RUN pip install -r requirements.txt
CMD ["lambda_file.lambda_handler"]
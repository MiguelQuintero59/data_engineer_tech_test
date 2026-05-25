FROM python:3.11

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /tech_test_vm

COPY pyproject.toml .

COPY . /tech_test_vm
RUN pip install --upgrade pip setuptools wheel
RUN pip install -e .


CMD ["transactions-etl"]
FROM python

WORKDIR /innowise_task_1/

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY data/ data/
COPY tests/ tests/
COPY config/ config/

RUN mkdir -p output

ENV PYTHONPATH=/innowise_task_1

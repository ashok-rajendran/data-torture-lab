FROM python:3.10-slim

WORKDIR /app

COPY flight_dataGen.py .

RUN pip install --no-cache-dir faker numpy

CMD ["python", "flight_dataGen.py"]

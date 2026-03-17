FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir setuptools wheel \
    && pip install --no-cache-dir streamlit==1.29.0 pandas==2.1.4 psycopg2-binary==2.9.9 plotly==5.17.0 numpy==1.26.4 requests==2.32.3 PyYAML==6.0.2

COPY dashboard.py /app/dashboard.py

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.address=0.0.0.0", "--server.port=8501"]


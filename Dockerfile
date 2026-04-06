FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir     fastapi uvicorn httpx     "redis[hiredis]" apscheduler feedparser     pdfplumber python-docx openpyxl Pillow

COPY monitor.py dashboard.py bid_engine.py muni_dashboard.py news_crawler.py      extract_all.py fetch_national.py      procurement_url_master_v2.csv ./
COPY run_daily.sh ./
RUN chmod +x run_daily.sh

# Create dirs
RUN mkdir -p /app/output /app/logs /app/data

ENV DATA_DIR=/app/data

EXPOSE 8007

# Dashboard runs as main process; scheduler runs inside via APScheduler
CMD ["python3", "dashboard.py"]

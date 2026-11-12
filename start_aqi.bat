@echo off
cd /d "C:\Users\janvi\OneDrive\Desktop\AQI Project"

echo Waiting for internet connection...
timeout /t 60 /nobreak > nul

echo Fetching the latest air quality data...
call venv\Scripts\activate
python ingestion\fetch_live_aqi.py >> logs\login_runs.log 2>&1

echo Starting the GitHub Actions runner...
cd /d C:\actions-runner
call run.cmd
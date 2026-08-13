# Starts all components of the Real-Time Event Detector in separate windows
Write-Host "Starting Big Data Architecture..." -ForegroundColor Cyan

# 1. Start FastAPI Backend
Start-Process -FilePath "python" -ArgumentList "-m", "uvicorn", "backend.main:app", "--reload", "--port", "8000"
Write-Host "Started Backend (Port 8000)" -ForegroundColor Green

# 2. Start Spark Consumer
Start-Process -FilePath "python" -ArgumentList "spark/spark_consumer.py"
Write-Host "Started Spark Streaming Engine" -ForegroundColor Green

# 3. Start Mock Producer (Feeds continuous fake data)
Start-Process -FilePath "python" -ArgumentList "producers/mock_producer.py"
Write-Host "Started Data Producer" -ForegroundColor Green

# 4. Start Vite Frontend
Start-Process -FilePath "npm.cmd" -ArgumentList "run", "dev" -WorkingDirectory "frontend"
Write-Host "Started Frontend (Port 5173)" -ForegroundColor Green

Write-Host "All services started! Please wait a few seconds for them to initialize." -ForegroundColor Yellow
Write-Host "Navigate to http://localhost:5173 in your browser." -ForegroundColor Cyan

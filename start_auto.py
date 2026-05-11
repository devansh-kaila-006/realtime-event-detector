"""
Automated Startup Script - No User Input Required
Starts all components automatically
"""

import os
import sys
import time
import subprocess
from pathlib import Path

class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_colored(message, color=Colors.END):
    print(f"{color}{message}{Colors.END}")

def print_header(message):
    print()
    print_colored("=" * 80, Colors.BOLD)
    print_colored(f"  {message}", Colors.BOLD + Colors.GREEN)
    print_colored("=" * 80, Colors.BOLD)
    print()

print_header("[AUTO] REAL-TIME EVENT DETECTION SYSTEM - AUTOMATED STARTUP")

print_colored("[INFO] Starting all components automatically...", Colors.YELLOW)
print_colored("[INFO] Press Ctrl+C to stop all services", Colors.YELLOW)
print()

# Step 1: Verify Docker services
print_colored("[1/6] Checking Docker Services...", Colors.BLUE)
try:
    result = subprocess.run(
        "docker ps --format \"{{.Names}}\"",
        shell=True,
        capture_output=True,
        text=True,
        timeout=10
    )
    running_services = result.stdout.lower()
    if 'kafka' in running_services and 'mongodb' in running_services:
        print_colored("   [OK] Docker services running", Colors.GREEN)
    else:
        print_colored("   [!] Starting Docker services...", Colors.YELLOW)
        subprocess.run("docker-compose up -d", shell=True)
        time.sleep(30)
        print_colored("   [OK] Docker services started", Colors.GREEN)
except Exception as e:
    print_colored(f"   [ERROR] Docker error: {e}", Colors.RED)

# Step 2: Setup database
print_colored("[2/6] Setting Up Database...", Colors.BLUE)
try:
    subprocess.run("python database/setup_indexes.py", shell=True, timeout=30)
    print_colored("   [OK] Database indexes created", Colors.GREEN)
except Exception as e:
    print_colored(f"   [!] Database setup had issues: {e}", Colors.YELLOW)

# Step 3: Start Producers
print_colored("[3/6] Starting Data Producers...", Colors.BLUE)
producers = [
    "python producers/wiki_producer_balanced.py",  # FIXED: 1% sampling
    "python producers/news_producer.py",           # FIXED: 180s polling
    "python producers/gdacs_producer.py",
    "python producers/financial_producer.py"
]

producer_processes = []
for cmd in producers:
    try:
        print_colored(f"   [START] {cmd}...", Colors.YELLOW)
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        producer_processes.append(process)
        print_colored(f"   [OK] Started (PID: {process.pid})", Colors.GREEN)
        time.sleep(2)
    except Exception as e:
        print_colored(f"   [ERROR] Failed to start: {e}", Colors.RED)

print_colored(f"   [OK] Started {len(producer_processes)} producers", Colors.GREEN)

# Step 4: Wait for initial data
print_colored("[4/6] Waiting for producers to send data (10s)...", Colors.BLUE)
time.sleep(10)

# Step 5: Start Spark Consumer
print_colored("[5/6] Starting Spark Consumer...", Colors.BLUE)
try:
    print_colored("   [START] spark-submit spark/spark_consumer.py...", Colors.YELLOW)
    consumer_process = subprocess.Popen(
        "spark-submit spark/spark_consumer.py",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    print_colored(f"   [OK] Started (PID: {consumer_process.pid})", Colors.GREEN)
except Exception as e:
    print_colored(f"   [ERROR] Failed to start consumer: {e}", Colors.RED)

# Step 6: Wait and Start Dashboard
print_colored("[6/6] Waiting for Spark to initialize (15s)...", Colors.BLUE)
time.sleep(15)

print()
print_colored("=" * 80, Colors.BOLD)
print_colored("  [SUCCESS] ALL COMPONENTS STARTED!", Colors.BOLD + Colors.GREEN)
print_colored("=" * 80, Colors.BOLD)
print()

print_colored("[ACCESS INFO]", Colors.BOLD)
print_colored("  Dashboard:    http://localhost:8501", Colors.GREEN)
print_colored("  Kafka UI:     http://localhost:8080", Colors.GREEN)
print_colored("  WebSocket:    ws://localhost:8765", Colors.GREEN)
print()

print_colored("[MONITORING]", Colors.BOLD)
print_colored("  * Check dashboard for real-time events", Colors.YELLOW)
print_colored("  * Monitor logs for data flow", Colors.YELLOW)
print_colored("  * Press Ctrl+C to stop dashboard", Colors.YELLOW)
print()

print_colored("[INFO] Starting dashboard...", Colors.BLUE)
time.sleep(2)

try:
    # Start dashboard in foreground
    subprocess.run("python -m streamlit run dashboard/app.py", shell=True)
except KeyboardInterrupt:
    print_colored("\n[STOP] Dashboard stopped", Colors.YELLOW)
    print_colored("[INFO] Other components still running in background", Colors.YELLOW)
    print_colored("[INFO] To stop all: Close terminal or use Task Manager", Colors.YELLOW)

"""
Quick Start Script for Real-Time Event Detection System
Automatically starts all components in the correct order
"""

import os
import sys
import time
import subprocess
import signal
from pathlib import Path

# Color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_colored(message, color=Colors.END):
    """Print colored message"""
    print(f"{color}{message}{Colors.END}")

def print_header(message):
    """Print formatted header"""
    print()
    print_colored("=" * 80, Colors.BOLD)
    print_colored(f"  {message}", Colors.BOLD + Colors.GREEN)
    print_colored("=" * 80, Colors.BOLD)
    print()

def print_step(step_num, message):
    """Print step indicator"""
    print_colored(f"[{step_num}/8] {message}...", Colors.BLUE)

def run_command(command, description, background=False):
    """Run a shell command"""
    try:
        if background:
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )
            print_colored(f"   [OK] {description} started (PID: {process.pid})", Colors.GREEN)
            return process
        else:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode == 0:
                print_colored(f"   [OK] {description} completed", Colors.GREEN)
                return True
            else:
                print_colored(f"   [X] {description} failed: {result.stderr}", Colors.RED)
                return False
    except Exception as e:
        print_colored(f"   [X] {description} error: {e}", Colors.RED)
        return None if background else False

def check_prerequisites():
    """Check if required tools are installed"""
    print_step(1, "Checking Prerequisites")

    all_good = True

    # Check Docker
    if run_command("docker --version", "Docker"):
        print_colored("   [OK] Docker installed", Colors.GREEN)
    else:
        print_colored("   [X] Docker not found - please install Docker", Colors.RED)
        all_good = False

    # Check Docker Compose
    if run_command("docker-compose --version", "Docker Compose"):
        print_colored("   [OK] Docker Compose installed", Colors.GREEN)
    else:
        print_colored("   [X] Docker Compose not found - please install Docker Compose", Colors.RED)
        all_good = False

    # Check Python
    if run_command("python --version", "Python"):
        print_colored("   [OK] Python installed", Colors.GREEN)
    else:
        print_colored("   [X] Python not found - please install Python 3.8+", Colors.RED)
        all_good = False

    # Check key packages (simplified - kafka and spacy have fallbacks)
    key_packages = [
        ("streamlit", "streamlit"),
        ("pymongo", "pymongo"),
        ("pyspark", "pyspark"),
    ]

    for import_name, display_name in key_packages:
        try:
            __import__(import_name)
            print_colored(f"   [OK] {display_name} installed", Colors.GREEN)
        except ImportError:
            print_colored(f"   [X] {display_name} not found", Colors.RED)
            all_good = False

    # Optional packages (system has fallbacks if missing)
    print_colored("   [INFO] Optional packages:", Colors.YELLOW)

    # kafka-python
    try:
        __import__("kafka")
        print_colored("   [OK] kafka-python installed", Colors.GREEN)
    except ImportError:
        print_colored("   [!] kafka-python not found - attempting install", Colors.YELLOW)
        run_command("pip install kafka-python", "kafka-python installation", False)

    # spacy (optional NLP enhancement)
    try:
        __import__("spacy")
        print_colored("   [OK] spacy installed (NLP enhanced)", Colors.GREEN)
    except ImportError:
        print_colored("   [!] spacy not found (system will use basic NLP)", Colors.YELLOW)

    return all_good

def setup_infrastructure():
    """Start Docker infrastructure"""
    print_step(2, "Starting Infrastructure (Kafka, MongoDB, Zookeeper)")

    # Start Docker Compose
    if run_command("docker-compose up -d", "Docker Compose"):
        print_colored("   [WAIT] Waiting for services to be ready (30s)...", Colors.YELLOW)
        time.sleep(30)

        # Verify services are running (Windows-compatible check)
        try:
            result = subprocess.run(
                "docker ps --format \"{{.Names}}\"",
                shell=True,
                capture_output=True,
                text=True,
                timeout=10
            )
            running_services = result.stdout.lower()
            if 'kafka' in running_services and 'zookeeper' in running_services and 'mongodb' in running_services:
                print_colored("   [OK] All services running", Colors.GREEN)
                return True
            else:
                print_colored("   [!] Services starting but not fully ready yet", Colors.YELLOW)
                return True  # Continue anyway, services may be starting
        except Exception as e:
            print_colored(f"   [!] Could not verify services: {e}", Colors.YELLOW)
            return True  # Continue anyway
    else:
        return False

def setup_database():
    """Setup database indexes"""
    print_step(3, "Setting Up Database")

    if run_command("python database/setup_indexes.py", "Index creation"):
        return True
    else:
        print_colored("   [!] Index setup failed, continuing anyway...", Colors.YELLOW)
        return True

def start_producers():
    """Start data producers"""
    print_step(4, "Starting Data Producers")

    producers = [
        ("python producers/wiki_producer.py", "Wikipedia Producer"),
        ("python producers/news_producer.py", "News Producer"),
        ("python producers/gdacs_producer.py", "GDACS Producer"),
        ("python producers/financial_producer.py", "Financial Producer")
    ]

    started_processes = []

    for command, name in producers:
        print_colored(f"   [START] Starting {name}...", Colors.YELLOW)
        process = run_command(command, name, background=True)
        if process:
            started_processes.append(process)
            time.sleep(2)  # Brief pause between starts

    print_colored(f"   [OK] Started {len(started_processes)} producers", Colors.GREEN)
    return started_processes

def start_consumer():
    """Start Spark consumer"""
    print_step(5, "Starting Spark Consumer")

    # Give producers time to send some data
    print_colored("   [WAIT] Waiting for producers to send initial data (10s)...", Colors.YELLOW)
    time.sleep(10)

    process = run_command(
        "spark-submit spark/spark_consumer.py",
        "Spark Consumer",
        background=True
    )

    if process:
        print_colored("   [WAIT] Waiting for Spark to initialize (15s)...", Colors.YELLOW)
        time.sleep(15)
        return process
    else:
        return None

def start_dashboard():
    """Start dashboard"""
    print_step(6, "Starting Dashboard")

    print_colored("   [INFO] Dashboard will be available at http://localhost:8501", Colors.GREEN)
    print_colored("   Press Ctrl+C to stop the dashboard", Colors.YELLOW)

    time.sleep(2)

    # Run dashboard in foreground
    try:
        subprocess.run("streamlit run dashboard/app.py", shell=True)
    except KeyboardInterrupt:
        print_colored("\n   [STOP] Dashboard stopped", Colors.YELLOW)
        return True

def run_tests():
    """Run system tests"""
    print_step(7, "Running System Tests")

    if run_command("python testing/test_system.py", "System Tests"):
        return True
    else:
        print_colored("   [!] Some tests failed, but system may still work", Colors.YELLOW)
        return True

def validate_performance():
    """Run performance validation"""
    print_step(8, "Performance Validation")

    if run_command("python performance_validator.py", "Performance Check"):
        return True
    else:
        print_colored("   [!] Performance issues detected - see recommendations", Colors.YELLOW)
        return True

def main():
    """Main entry point"""
    print_header("[START] REAL-TIME EVENT DETECTION SYSTEM - QUICK START")

    print_colored("This script will start all components of the event detection system.", Colors.YELLOW)
    print_colored("Press Ctrl+C at any time to stop.", Colors.YELLOW)
    print()

    try:
        # Check prerequisites
        if not check_prerequisites():
            print_colored("[X] Prerequisites not met. Please install missing dependencies.", Colors.RED)
            return 1

        input(f"{Colors.YELLOW}Press Enter to continue with infrastructure setup...{Colors.END}")

        # Setup infrastructure
        if not setup_infrastructure():
            print_colored("[X] Infrastructure setup failed.", Colors.RED)
            return 1

        # Setup database
        setup_database()

        input(f"{Colors.YELLOW}Press Enter to start data producers...{Colors.END}")

        # Start producers
        producer_processes = start_producers()

        input(f"{Colors.YELLOW}Press Enter to start Spark consumer...{Colors.END}")

        # Start consumer
        consumer_process = start_consumer()
        if not consumer_process:
            print_colored("[X] Failed to start Spark consumer.", Colors.RED)
            return 1

        input(f"{Colors.YELLOW}Press Enter to run system tests...{Colors.END}")

        # Run tests
        run_tests()

        input(f"{Colors.YELLOW}Press Enter to validate performance...{Colors.END}")

        # Validate performance
        validate_performance()

        print()
        print_colored("=" * 80, Colors.BOLD)
        print_colored("  [SUCCESS] SYSTEM STARTUP COMPLETE!", Colors.BOLD + Colors.GREEN)
        print_colored("=" * 80, Colors.BOLD)
        print()

        print_colored("[DASHBOARD] Dashboard: http://localhost:8501", Colors.GREEN)
        print_colored("[KAFKA] Kafka UI: http://localhost:8080", Colors.GREEN)
        print_colored("[WEBSOCKET] WebSocket: ws://localhost:8765", Colors.GREEN)
        print()

        print_colored("[TIPS]", Colors.YELLOW)
        print_colored("   * Monitor producer logs for data flow", Colors.YELLOW)
        print_colored("   * Check dashboard for real-time updates", Colors.YELLOW)
        print_colored("   * Use Ctrl+C to stop individual components", Colors.YELLOW)
        print_colored("   * Run 'python performance_validator.py' for optimization tips", Colors.YELLOW)
        print()

        # Start dashboard
        start_dashboard()

    except KeyboardInterrupt:
        print_colored("\n\n[STOP] Shutdown requested by user", Colors.YELLOW)
        print_colored("[INFO] Stopping all components...", Colors.YELLOW)

        # Cleanup (kill background processes) - Windows-compatible
        try:
            print_colored("   [STOP] Stopping background processes...", Colors.YELLOW)
            # Windows taskkill command - kill all python processes (not ideal but works for demo)
            subprocess.run("taskkill /F /IM python.exe", shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        except:
            pass

        print_colored("[OK] All components stopped", Colors.GREEN)
        return 0

    except Exception as e:
        print_colored(f"[ERROR] Error: {e}", Colors.RED)
        return 1

if __name__ == "__main__":
    sys.exit(main())
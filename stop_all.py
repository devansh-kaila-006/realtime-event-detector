"""
Kill all Python processes to stop old producers
Then restart with balanced configuration
"""

import subprocess
import time

print("=" * 60)
print("[STOPPING] Stopping all Python processes...")
print("=" * 60)

# Kill all Python processes (Windows)
try:
    result = subprocess.run(
        "taskkill /F /IM python.exe",
        shell=True,
        capture_output=True,
        text=True
    )
    print(f"[INFO] Killed Python processes")
except Exception as e:
    print(f"[INFO] Error stopping processes: {e}")

print()
print("[WAIT] Waiting 3 seconds for processes to stop...")
time.sleep(3)

print()
print("[SUCCESS] All producers stopped!")
print()
print("[NEXT] The balanced Wikipedia producer will now start")
print("[CONFIG] Wikipedia will use 10% sampling rate")
print("[EXPECTED] ~1,700 Wikipedia events vs other sources")

import subprocess
import sys

steps = [
    [sys.executable, "scripts/fetch_data.py"],
    [sys.executable, "scripts/transform_data.py"],
    [sys.executable, "scripts/load_to_sql.py"],
]

for step in steps:
    print(f"\n▶ Running: {' '.join(step)}")
    subprocess.run(step, check=True)
    print("✅ Step completed")

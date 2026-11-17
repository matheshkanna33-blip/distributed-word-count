"""
One-command demo: starts 1 master + N workers on this machine.
One worker is set to crash on purpose so you can see fault tolerance.

    python run_demo.py --workers 4
"""
import argparse
import subprocess
import sys
import time

p = argparse.ArgumentParser()
p.add_argument("--workers", type=int, default=4)
p.add_argument("--no-crash", action="store_true", help="don't simulate a crash")
a = p.parse_args()

master = subprocess.Popen([sys.executable, "master.py"])
time.sleep(1)

workers = []
for i in range(1, a.workers + 1):
    cmd = [sys.executable, "worker.py", "--name", f"worker-{i}"]
    if i == 1 and not a.no_crash:
        cmd += ["--fail-after", "2"]
    workers.append(subprocess.Popen(cmd))

master.wait()
for w in workers:
    w.wait(timeout=5)

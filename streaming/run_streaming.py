"""
run_streaming.py
─────────────────────────────────────────────────────────────────────────────
Launches all three Spark Structured Streaming jobs in separate OS processes.

Each job runs as an independent Python process — not a thread — because
each creates its own SparkSession. Running them in separate processes
avoids SparkContext conflicts and allows independent restarts.

Usage:
    # Run all three jobs
    python streaming/run_streaming.py

    # Run a single job
    python streaming/run_streaming.py --job stock_aggregator
    python streaming/run_streaming.py --job threshold_detector
    python streaming/run_streaming.py --job shipment_monitor

For production (inside Docker), use spark-submit instead:
    See streaming/submit_jobs.sh
"""

import argparse
import logging
import subprocess
import sys
import os
import signal
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("StreamingRunner")

BASE = os.path.dirname(os.path.abspath(__file__))

JOBS = {
    "stock_aggregator":    os.path.join(BASE, "jobs", "stock_level_aggregator.py"),
    "threshold_detector":  os.path.join(BASE, "jobs", "threshold_alert_detector.py"),
    "shipment_monitor":    os.path.join(BASE, "jobs", "shipment_delay_monitor.py"),
}


def launch_job(name: str, script: str) -> subprocess.Popen:
    logger.info("Launching job: %s → %s", name, script)
    proc = subprocess.Popen(
        [sys.executable, script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    logger.info("  PID %d assigned to %s", proc.pid, name)
    return proc


def stream_output(proc: subprocess.Popen, name: str):
    """Log subprocess stdout in real time."""
    for line in proc.stdout:
        print(f"[{name}] {line}", end="")


def main(job_name: str | None = None):
    jobs_to_run = {job_name: JOBS[job_name]} if job_name else JOBS

    processes = {}
    for name, script in jobs_to_run.items():
        processes[name] = launch_job(name, script)
        time.sleep(2)   # stagger starts to avoid Kafka connection storms

    logger.info("All streaming jobs launched. Press Ctrl+C to stop.")

    def shutdown(sig, frame):
        logger.info("Shutdown signal received. Terminating all jobs...")
        for name, proc in processes.items():
            logger.info("  Stopping %s (PID %d)...", name, proc.pid)
            proc.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Monitor processes; restart if any crash
    while True:
        for name, proc in list(processes.items()):
            ret = proc.poll()
            if ret is not None:
                logger.warning("Job %s exited with code %d. Restarting...", name, ret)
                processes[name] = launch_job(name, JOBS[name])
        time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job", choices=list(JOBS.keys()), default=None,
        help="Name of a single job to run. Omit to run all three."
    )
    args = parser.parse_args()
    main(args.job)

"""
Worker node.

Connects to the master, repeatedly asks for a task, runs the MAP step
(word count on its chunk) and sends the partial result back.
Run as many workers as you like — on the same machine or other machines.
"""
import argparse
import os
import socket
import time

from common import DEFAULT_HOST, DEFAULT_PORT, send_msg, recv_msg, map_word_count


def run_worker(name, host, port, delay, fail_after):
    for attempt in range(20):  # retry until master is up
        try:
            sock = socket.create_connection((host, port))
            break
        except OSError:
            time.sleep(0.5)
    else:
        print(f"[{name}] Could not reach master at {host}:{port}")
        return

    f = sock.makefile("rwb")
    send_msg(f, {"type": "hello", "name": name})
    print(f"[{name}] Connected to master {host}:{port}")
    done = 0

    while True:
        msg = recv_msg(f)
        if msg is None or msg["type"] == "shutdown":
            print(f"[{name}] Shutting down after {done} tasks")
            break
        if msg["type"] == "wait":
            send_msg(f, {"type": "ready"})
            continue
        if msg["type"] == "task":
            if fail_after is not None and done >= fail_after:
                print(f"[{name}] Simulating crash on task {msg['task_id']}!")
                os._exit(1)
            counts = map_word_count(msg["data"])
            time.sleep(delay)  # simulate heavier work
            send_msg(f, {"type": "result", "task_id": msg["task_id"],
                         "counts": counts})
            done += 1
    sock.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Distributed word count — worker node")
    p.add_argument("--name", default=f"worker-{os.getpid()}")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--delay", type=float, default=0.1,
                   help="extra seconds per task to simulate heavy work")
    p.add_argument("--fail-after", type=int, default=None,
                   help="crash after N tasks (to test fault tolerance)")
    a = p.parse_args()
    run_worker(a.name, a.host, a.port, a.delay, a.fail_after)

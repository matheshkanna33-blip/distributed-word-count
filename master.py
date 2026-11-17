"""
Master node.

- Splits the input file into chunks (tasks).
- Accepts any number of worker connections over TCP.
- Hands out tasks to idle workers (dynamic load balancing).
- If a worker dies mid-task, its task is put back in the queue (fault tolerance).
- When all tasks finish, runs the REDUCE step and prints the result.
"""
import argparse
import socket
import threading
import time
from collections import Counter, deque

from common import DEFAULT_HOST, DEFAULT_PORT, send_msg, recv_msg


class Master:
    def __init__(self, input_file, chunk_lines, host, port, top_n):
        self.host, self.port, self.top_n = host, port, top_n
        self.tasks = self._split(input_file, chunk_lines)
        self.pending = deque(self.tasks.keys())   # task ids not yet assigned
        self.in_progress = {}                     # task_id -> worker name
        self.results = {}                         # task_id -> partial counts
        self.worker_stats = Counter()             # worker name -> tasks done
        self.lock = threading.Lock()
        self.done = threading.Event()

    @staticmethod
    def _split(path, chunk_lines):
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
        tasks = {}
        for i in range(0, len(lines), chunk_lines):
            tasks[len(tasks)] = "".join(lines[i:i + chunk_lines])
        print(f"[master] Split '{path}' into {len(tasks)} tasks")
        return tasks

    # ---------- task scheduling ----------
    def _next_task(self, worker):
        with self.lock:
            if not self.pending:
                return None
            task_id = self.pending.popleft()
            self.in_progress[task_id] = worker
            return task_id

    def _complete(self, task_id, counts, worker):
        with self.lock:
            if task_id in self.results:
                return  # duplicate result, ignore
            self.in_progress.pop(task_id, None)
            self.results[task_id] = counts
            self.worker_stats[worker] += 1
            print(f"[master] Task {task_id} done by {worker} "
                  f"({len(self.results)}/{len(self.tasks)})")
            if len(self.results) == len(self.tasks):
                self.done.set()

    def _requeue_worker_tasks(self, worker):
        with self.lock:
            lost = [t for t, w in self.in_progress.items() if w == worker]
            for t in lost:
                del self.in_progress[t]
                self.pending.appendleft(t)
            if lost:
                print(f"[master] {worker} failed, re-queued tasks {lost}")

    # ---------- per-worker connection ----------
    def _handle_worker(self, conn, addr):
        f = conn.makefile("rwb")
        worker = f"{addr[0]}:{addr[1]}"
        try:
            hello = recv_msg(f)
            if hello and hello.get("type") == "hello":
                worker = hello.get("name", worker)
            print(f"[master] Worker connected: {worker}")

            while not self.done.is_set():
                task_id = self._next_task(worker)
                if task_id is None:
                    # Nothing to give right now; others may still fail and requeue.
                    send_msg(f, {"type": "wait"})
                    msg = recv_msg(f)
                    if msg is None:
                        break
                    time.sleep(0.2)
                    continue

                send_msg(f, {"type": "task", "task_id": task_id,
                             "data": self.tasks[task_id]})
                msg = recv_msg(f)
                if msg is None:
                    raise ConnectionError("worker disconnected")
                if msg.get("type") == "result":
                    self._complete(msg["task_id"], msg["counts"], worker)

            send_msg(f, {"type": "shutdown"})
        except (ConnectionError, OSError, ValueError) as e:
            print(f"[master] Lost {worker}: {e}")
        finally:
            self._requeue_worker_tasks(worker)
            try:
                conn.close()
            except OSError:
                pass

    # ---------- main loop ----------
    def run(self):
        start = time.time()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen()
        server.settimeout(0.5)
        print(f"[master] Listening on {self.host}:{self.port} — start workers now")

        while not self.done.is_set():
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue
            threading.Thread(target=self._handle_worker, args=(conn, addr),
                             daemon=True).start()

        time.sleep(0.5)  # let workers receive shutdown
        server.close()
        self._reduce_and_report(time.time() - start)

    def _reduce_and_report(self, elapsed):
        # REDUCE step: merge all partial counts
        total = Counter()
        for partial in self.results.values():
            total.update(partial)

        print("\n========== RESULT ==========")
        print(f"Total words   : {sum(total.values())}")
        print(f"Unique words  : {len(total)}")
        print(f"Time taken    : {elapsed:.2f}s")
        print("\nTasks per worker:")
        for w, n in self.worker_stats.most_common():
            print(f"  {w:<12} {n}")
        print(f"\nTop {self.top_n} words:")
        for word, n in total.most_common(self.top_n):
            print(f"  {word:<15} {n}")

        with open("output.txt", "w", encoding="utf-8") as out:
            for word, n in total.most_common():
                out.write(f"{word}\t{n}\n")
        print("\nFull result written to output.txt")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Distributed word count — master node")
    p.add_argument("--input", default="data/sample.txt")
    p.add_argument("--chunk-lines", type=int, default=1000)
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--top", type=int, default=10)
    a = p.parse_args()
    Master(a.input, a.chunk_lines, a.host, a.port, a.top).run()

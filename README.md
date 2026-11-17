# Distributed Word Count (Mini MapReduce) — Python

A mini **distributed computing** project that counts words in a large text file
by splitting the work across multiple **worker nodes** coordinated by a **master node**
over TCP sockets. It follows the **MapReduce** model.

## Features
- **Master–Worker architecture** over TCP sockets
- **Map step** on workers, **Reduce step** on master
- **Dynamic load balancing** — idle workers pull the next task
- **Fault tolerance** — if a worker crashes, its task is re-assigned to another worker
- Works on **one machine or across multiple machines** on a network
- Pure Python 3 — no external libraries needed

## Architecture
```
                 +-------------------+
   sample.txt -> |      MASTER       |  split into chunks (tasks)
                 |  task queue       |  REDUCE: merge partial counts
                 +---------+---------+
            TCP  /         |         \  TCP
       +--------+    +--------+    +--------+
       |Worker 1|    |Worker 2|    |Worker N|   MAP: count words in chunk
       +--------+    +--------+    +--------+
```

## Project structure
| File | Purpose |
|------|---------|
| `master.py` | Splits input, schedules tasks, handles failures, reduces results |
| `worker.py` | Connects to master, runs map step on each chunk |
| `common.py` | Message protocol (JSON over TCP) + map function |
| `run_demo.py` | Starts master + N workers locally in one command |
| `data/sample.txt` | Sample input (20,000 lines) |

## How to run

**Requirement:** Python 3.8+

### Option 1 — one command demo
```bash
python run_demo.py --workers 4
```
Worker-1 deliberately crashes after 2 tasks so you can see the master re-assign its work.

### Option 2 — separate terminals (shows the distribution clearly)
```bash
# Terminal 1
python master.py --input data/sample.txt --chunk-lines 1000

# Terminal 2, 3, 4 ...
python worker.py --name worker-1
python worker.py --name worker-2
python worker.py --name worker-3 --fail-after 2   # test fault tolerance
```

### Option 3 — multiple computers on same Wi-Fi/LAN
```bash
# On master PC (find its IP with ipconfig / ifconfig)
python master.py --host 0.0.0.0

# On each other PC
python worker.py --host <MASTER_IP> --name laptop-2
```

## Sample output
```
========== RESULT ==========
Total words   : 240000
Unique words  : 22
Time taken    : 2.53s

Tasks per worker:
  worker-4     6
  worker-3     6
  worker-2     6
  worker-1     2

Top 10 words:
  master          11124
  data            11086
  ...
```
Full result is saved to `output.txt`.

## Distributed computing concepts shown
- Client–server communication (sockets)
- Task decomposition & parallelism
- Load balancing (pull-based scheduling)
- Fault tolerance (task re-execution)
- MapReduce (map on workers, reduce on master)

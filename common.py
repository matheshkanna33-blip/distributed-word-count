"""Shared helpers: newline-delimited JSON messages over TCP sockets."""
import json
import re

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000
WORD_RE = re.compile(r"[a-zA-Z']+")


def send_msg(sock_file, msg: dict) -> None:
    """Send one JSON message terminated by a newline."""
    sock_file.write((json.dumps(msg) + "\n").encode("utf-8"))
    sock_file.flush()


def recv_msg(sock_file):
    """Receive one JSON message. Returns None if the connection closed."""
    line = sock_file.readline()
    if not line:
        return None
    return json.loads(line.decode("utf-8"))


def map_word_count(text: str) -> dict:
    """MAP step: count words in a chunk of text."""
    counts = {}
    for word in WORD_RE.findall(text.lower()):
        counts[word] = counts.get(word, 0) + 1
    return counts

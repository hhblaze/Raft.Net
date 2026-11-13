"""Exercise the book's actual multi-process example; no external Python packages.

Build TutorialNode in Release first. All generated data stays under the supplied
output folder (default: RaftNet8/.artifacts/human-tutorial/examples-<timestamp>).
Processes started here are the only processes this script stops.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import queue
import re
import secrets
import socket
import subprocess
import threading
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DLL = HERE / "examples/TutorialNode/bin/Release/net8.0/Raft.TutorialNode.dll"


class Node:
    def __init__(self, node_id, data, port, out, *, memory=False, single=False, label=""):
        self.node_id = node_id
        self.lines = queue.Queue()
        self.log = (out / f"{label}{node_id}.log").open("w", encoding="utf-8")
        args = ["dotnet", str(DLL), node_id, "--data", str(data), "--base-port", str(port)]
        if memory:
            args.append("--memory")
        if single:
            args.append("--single")
        self.process = subprocess.Popen(args, cwd=ROOT, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        text=True, encoding="utf-8", errors="replace", bufsize=1)
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()
        try:
            self.until(("STARTED ",), 30)
        except BaseException:
            self.stop(kill=True)
            raise

    def _read(self):
        try:
            for line in self.process.stdout:
                self.log.write(line)
                self.log.flush()
                self.lines.put(line.rstrip("\r\n"))
        finally:
            self.lines.put(None)

    def until(self, prefixes, timeout=25):
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{self.node_id}: waiting for {prefixes}")
            try:
                line = self.lines.get(timeout=remaining)
            except queue.Empty:
                raise TimeoutError(f"{self.node_id}: waiting for {prefixes}") from None
            if line is None:
                raise RuntimeError(f"{self.node_id} exited {self.process.poll()}; see {self.log.name}")
            if line.startswith(prefixes):
                return line

    def ask(self, command, prefixes, timeout=25):
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()
        return self.until(prefixes, timeout)

    def status(self):
        line = self.ask("status", ("STATUS ",))
        return dict(re.findall(r"(\w+)=([^ ]+)", line))

    def stop(self, *, kill=False):
        if self.process.poll() is None:
            if kill:
                self.process.kill()
            else:
                self.process.stdin.write("quit\n")
                self.process.stdin.flush()
            try:
                self.process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
                raise
        self.thread.join(timeout=5)
        self.log.close()
        if not kill and self.process.returncode != 0:
            raise RuntimeError(f"{self.node_id}: orderly exit code {self.process.returncode}")


def free_ports(count):
    for _ in range(100):
        # Stay below common ephemeral TCP ranges: an earlier node must not use
        # a later listener's port as an outbound ephemeral source before it binds.
        base = 10000 + secrets.randbelow(20000)
        sockets = []
        try:
            for port in range(base, base + count):
                current = socket.socket()
                sockets.append(current)
                current.bind(("127.0.0.1", port))
            return base
        except OSError:
            pass
        finally:
            for current in sockets:
                current.close()
    raise RuntimeError("No adjacent free loopback ports")


def find_leader(nodes):
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        leaders = [node for node in nodes.values() if node.status()["role"] == "Leader"]
        if len(leaders) == 1:
            result = leaders[0].ask("get __readiness__", ("VALUE ", "NOT_LEADER ", "TIMEOUT "))
            if result.startswith("VALUE "):
                return leaders[0]
        time.sleep(0.1)
    raise TimeoutError("No ready leader")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    out = (args.out or ROOT / ".artifacts/human-tutorial" / f"examples-{stamp}").resolve()
    out.mkdir(parents=True, exist_ok=False)
    if not DLL.is_file():
        raise FileNotFoundError("Build the Release TutorialNode first")
    alive = {}
    all_nodes = []
    checks = []

    def check(name, condition):
        if not condition:
            raise AssertionError(name)
        checks.append(name)
        print(f"PASS {name}", flush=True)

    def start(node_id, data, port, label, **kwargs):
        node = Node(node_id, data, port, out, label=label, **kwargs)
        alive[node_id] = node
        all_nodes.append(node)
        return node

    def stop(node_id, **kwargs):
        alive.pop(node_id).stop(**kwargs)

    started = time.monotonic()
    try:
        port = free_ports(3)
        data = out / "disk"
        for node_id in ("n1", "n2", "n3"):
            start(node_id, data, port, "disk-initial-")
        leader = find_leader(alive)
        check("three independent processes elect a ready leader", leader is not None)
        first = leader.ask("put req-001 greeting first", ("OK ",))
        original = re.search(r"originalIndex=(\d+)", first).group(1)
        repeated = leader.ask("put req-001 greeting first", ("OK ",))
        check("same-ID retry preserves original result", f"originalIndex={original}" in repeated)
        conflict = leader.ask("put req-001 other wrong", ("CONFLICT ",))
        check("conflicting request ID has no second business effect", conflict.startswith("CONFLICT ") and
              leader.ask("get other", ("VALUE ",)).startswith("VALUE null "))
        leader.ask("put req-unicode привет значение по-русски", ("OK ",))
        unicode_value = leader.ask("get привет", ("VALUE ",))
        check("Cyrillic command and result round-trip", json.loads(unicode_value[6:].rsplit(" fence=", 1)[0]) == "значение по-русски")
        image = leader.ask("snapshot", ("SNAPSHOT ",), 40)
        check("disk generation publishes a snapshot", "index=" in image)
        old_leader = leader.node_id
        stop(old_leader, kill=True)
        leader = find_leader(alive)
        leader.ask("put req-002 greeting after-crash", ("OK ",))
        check("quorum survives abrupt leader process kill", leader.ask("get greeting", ("VALUE ",)).startswith('VALUE "after-crash" '))
        restarted = start(old_leader, data, port, "disk-crash-restart-")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if restarted.ask("local greeting", ("LOCAL ",)).startswith('LOCAL "after-crash" '):
                break
            time.sleep(0.1)
        else:
            raise AssertionError("crashed node does not catch up")
        check("crashed disk voter restarts and catches up", True)
        leader = find_leader(alive)
        followers = [node_id for node_id in alive if node_id != leader.node_id]
        for node_id in followers:
            stop(node_id, kill=True)
        unavailable = leader.ask("put req-noquorum unavailable value", ("OK ", "NOT_LEADER ", "TIMEOUT "))
        check("minority cannot acknowledge a new write", not unavailable.startswith("OK "))
        read = leader.ask("get greeting", ("VALUE ", "NOT_LEADER ", "TIMEOUT "))
        check("minority cannot confirm a fresh read", not read.startswith("VALUE "))
        for node_id in followers:
            start(node_id, data, port, "disk-quorum-restored-")
        find_leader(alive)
        for node_id in list(alive):
            stop(node_id)
        for node_id in ("n1", "n2", "n3"):
            start(node_id, data, port, "disk-full-restart-")
        leader = find_leader(alive)
        check("all-node clean restart preserves business data", leader.ask("get greeting", ("VALUE ",)).startswith('VALUE "after-crash" '))
        repeated = leader.ask("put req-001 greeting first", ("OK ",))
        check("deduplication survives full restart", f"originalIndex={original}" in repeated and
              leader.ask("get greeting", ("VALUE ",)).startswith('VALUE "after-crash" '))
        for node_id in list(alive):
            stop(node_id)
        memory_root = out / "memory-data-must-not-exist"
        memory_port = free_ports(1)
        memory = start("n1", memory_root, memory_port, "memory-initial-", memory=True, single=True)
        find_leader(alive)
        memory.ask("put req-ram greeting volatile", ("OK ",))
        check("memory node applies and reads", memory.ask("get greeting", ("VALUE ",)).startswith('VALUE "volatile" '))
        stop("n1")
        memory = start("n1", memory_root, memory_port, "memory-reset-", memory=True, single=True)
        find_leader(alive)
        check("memory restart loses values and has no database root", memory.ask("get greeting", ("VALUE ",)).startswith("VALUE null ") and not memory_root.exists())
        stop("n1")
        report = {"passed": True, "checks": checks, "elapsed_seconds": round(time.monotonic() - started, 2),
                  "process_starts": len(all_nodes), "artifact_root": str(out)}
        (out / "result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report), flush=True)
    finally:
        for node in list(alive.values()):
            try:
                node.stop(kill=True)
            except Exception:
                pass


if __name__ == "__main__":
    main()

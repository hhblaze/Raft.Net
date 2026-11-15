"""Verify independent server/client processes. Retain only this run's data/logs under --root."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import secrets
import socket
import subprocess
import threading
import time
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
DLL = HERE / "bin/Release/net8.0/Raft.Cluster.Demo.dll"
FLAGS = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


class Process:
    def __init__(self, args, log):
        self.lines = queue.Queue()
        self.log = log.open("w", encoding="utf-8")
        self.process = subprocess.Popen(["dotnet", str(DLL), *args], cwd=ROOT,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1, creationflags=FLAGS)
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()

    def read(self):
        try:
            for line in self.process.stdout:
                self.log.write(line); self.log.flush(); self.lines.put(line.rstrip())
        finally:
            self.lines.put(None)

    def until(self, predicate, timeout=30):
        deadline = time.monotonic() + timeout
        while True:
            try:
                line = self.lines.get(timeout=max(.01, deadline - time.monotonic()))
            except queue.Empty:
                raise TimeoutError(f"Timed out; see {self.log.name}") from None
            if line is None:
                raise RuntimeError(f"Process exited {self.process.poll()}; see {self.log.name}")
            if predicate(line):
                return line
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out; see {self.log.name}")

    def send(self, line):
        self.process.stdin.write(line + "\n"); self.process.stdin.flush()

    def stop(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=15); self.thread.join(timeout=5)
        self.log.close()


def reserve_ports():
    for _ in range(100):
        base = 20000 + secrets.randbelow(20000)
        sockets = []
        try:
            for port in [*range(base, base + 9), *range(base + 20, base + 29)]:
                sock = socket.socket(); sockets.append(sock); sock.bind(("127.0.0.1", port))
            return base, base + 20
        except OSError:
            pass
        finally:
            for sock in sockets: sock.close()
    raise RuntimeError("No free test port block")


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    output = args.root.resolve() / ("client-processes-" + time.strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(4))
    output.mkdir(parents=True, exist_ok=False)
    peer, client = reserve_ports()
    group = "process_" + secrets.token_hex(5)
    common = ["--root", str(output / "nodes"), "--group", group, "--peer-base-port", str(peer), "--client-base-port", str(client)]
    seeds = ",".join(f"http://127.0.0.1:{client + index}/" for index in range(3))
    external = ["--group", group, "--seeds", seeds]
    nodes = {}; processes = []
    report = {"Passed": False, "DataRoot": str(output), "IndependentProcesses": True, "OriginalPids": {}}
    counter = 0

    def start_node(node_id):
        child = Process(["node", node_id, *common], output / f"{node_id}-{secrets.token_hex(3)}.log")
        processes.append(child); nodes[node_id] = child
        child.until(lambda line: line.startswith("NODE "))
        return child

    def status(child):
        child.send("status")
        return json.loads(child.until(lambda line: line.startswith('{"NodeId"')))

    def leader(exclude=None):
        end = time.monotonic() + 30
        while time.monotonic() < end:
            for node_id, child in list(nodes.items()):
                if node_id != exclude and child.process.poll() is None:
                    state = status(child)
                    if state["Role"] == 2 and state["CommitIndex"] > 0: return node_id, child
            time.sleep(.1)
        raise TimeoutError("No elected leader")

    def cli(command):
        nonlocal counter
        counter += 1
        result = subprocess.run(["dotnet", str(DLL), *command, *external], cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", timeout=60, creationflags=FLAGS)
        (output / f"client-{counter:02}.log").write_text(result.stdout, encoding="utf-8")
        if result.returncode: raise RuntimeError(f"Client failed: {result.stdout[-3000:]}")
        return result.stdout

    def result_id(output):
        return next(line for line in reversed(output.splitlines()) if line.isdecimal())

    try:
        for node_id in ("n1", "n2", "n3"):
            child = start_node(node_id); report["OriginalPids"][node_id] = child.process.pid
        first_leader, first = leader()
        subscriber = Process(["subscribe", "--entity", "events", "--ids", "--after", "0", *external], output / "subscriber.log")
        processes.append(subscriber)
        for value in range(1, 4):
            assert result_id(cli(["publish", "--entity", "events", "--text", str(value)])) == str(value)
            subscriber.until(lambda line, value=value: line == f"COMMITTED events/{value}")
        first.stop()  # Actual abrupt OS-process death, not graceful node disposal.
        nodes.pop(first_leader)
        leader()
        assert result_id(cli(["publish", "--entity", "events", "--text", "after-crash"])) == "4"
        subscriber.until(lambda line: line == "COMMITTED events/4")
        start_node(first_leader)
        old_pids = {key: child.process.pid for key, child in nodes.items()}
        start_node("n4")
        current_id, current = leader()
        operation = str(uuid.uuid4()); current.send(f"add n4 {operation}")
        added = json.loads(current.until(lambda line: line.startswith('{"OperationId"')))
        assert added["Stage"] == 6, added
        subscriber.until(lambda line: line.startswith("TOPOLOGY ") and '"NodeId":"n4"' in line)
        assert all(nodes[node_id].process.pid == pid for node_id, pid in old_pids.items())
        removal = str(uuid.uuid4()); current.send(f"remove {current_id} {removal}")
        reply = current.until(lambda line: line.startswith('{"OperationId"') or line.startswith("ERROR "))
        while reply.startswith("ERROR "):
            _, next_leader = leader(exclude=current_id)
            next_leader.send(f"remove {current_id} {removal}")
            reply = next_leader.until(lambda line: line.startswith('{"OperationId"') or line.startswith("ERROR "))
        assert json.loads(reply)["Stage"] == 6
        nodes.pop(current_id).stop()
        assert result_id(cli(["publish", "--entity", "events", "--text", "after-remove"])) == "5"
        subscriber.until(lambda line: line == "COMMITTED events/5")
        subscriber.stop()
        resumed = Process(["subscribe", "--entity", "events", "--ids", "--after", "5", *external], output / "subscriber-resumed.log")
        processes.append(resumed)
        assert result_id(cli(["publish", "--entity", "events", "--text", "resume"])) == "6"
        resumed.until(lambda line: line == "COMMITTED events/6")
        source = output / "source.bin"; source.write_bytes(os.urandom(2 * 1024 * 1024 + 123))
        manifest = result_id(cli(["upload", "--file", str(source), "--chunk-bytes", "1048576"]))
        destination = output / "download.bin"
        cli(["download", "--manifest", manifest, "--file", str(destination)])
        assert hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(destination.read_bytes()).digest()
        report.update(Passed=True, FinalMessages=6, FileBytes=source.stat().st_size, ManifestId=int(manifest),
            AbruptLeaderCrash=True, AddWithoutRestart=True, LeaderRemoval=True, CursorResume=True, FileSha256=True)
    finally:
        for child in processes:
            if not child.log.closed: child.stop()
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()

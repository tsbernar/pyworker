import atexit
import subprocess
import sys
import time
from collections import deque

import zmq

import pyworker.worker
from pyworker import config


def start_workers(num_workers: int = config.NUM_WORKERS) -> list[zmq.Socket]:
    context = zmq.Context()
    connections: list[zmq.Socket] = []
    procs: list[subprocess.Popen] = []

    for worker_num in range(num_workers):
        proc = subprocess.Popen(
            [sys.executable, pyworker.worker.__file__, str(worker_num)]
        )
        procs.append(proc)

        port = config.WORKER_PORT_START + worker_num
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://localhost:{port}")
        connections.append(socket)

    def cleanup(procs: list[subprocess.Popen]):
        for p in procs:
            p.kill()

    atexit.register(cleanup, procs)

    return connections


def run() -> None:
    connections = start_workers()

    start = time.time()

    for day in range(config.BACKTEST_DAYS):
        print(f"Starting day {day} of backtest")
        for worker in connections:
            worker.send(f"Do some work for day {day}".encode())

        still_working = deque(connections)

        while still_working:
            worker = still_working.pop()
            try:
                message = worker.recv(flags=zmq.NOBLOCK)
                print(message)
            except zmq.Again as e:
                # Not ready yet
                still_working.appendleft(worker)

    print(
        f"Backtest for {config.NUM_WORKERS} strategies over {config.BACKTEST_DAYS} days with runtime per day per strategy of {config.SIMULATED_STRATEGY_RUNTIME_MS}ms took {time.time() - start}s"
    )
    print(
        f"Running in sequence would have taken {config.NUM_WORKERS*config.BACKTEST_DAYS*config.SIMULATED_STRATEGY_RUNTIME_MS/1000}s"
    )


if __name__ == "__main__":
    run()

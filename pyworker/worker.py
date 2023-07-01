import sys
import time

import zmq

from pyworker import config


def run(worker_num: int) -> None:
    context = zmq.Context()
    # using REQ/REP pattern: https://zeromq.org/socket-api/#request-reply-pattern
    # note: could use dealer / router pattern for async implementation
    socket = context.socket(zmq.REP)

    port = config.WORKER_PORT_START + worker_num
    socket.bind(f"tcp://*:{port}")

    while True:
        #  Wait for next request from client
        message = socket.recv().decode()
        print(f"Worker {worker_num} received request: {message}")

        #  Do some 'work'
        time.sleep(config.SIMULATED_STRATEGY_RUNTIME_MS / 1000)

        #  Send reply back to client
        socket.send(f"Finished work for {message}".encode())


if __name__ == "__main__":
    worker_num = int(sys.argv[1])
    print(f"Starting worker {worker_num}")
    run(worker_num=worker_num)

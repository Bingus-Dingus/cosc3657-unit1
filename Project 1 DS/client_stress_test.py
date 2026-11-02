

import socket
import threading
import time

from protocol import recv_message, send_message

NUM_WORKERS = 20
NUM_KEYS = 25
failures = []


def request(sock, msg):
    send_message(sock, msg)
    return recv_message(sock)


def worker(n):
    name = f"[Worker Thread {n:02d}]"
    try:
        sock = socket.create_connection(("127.0.0.1", 9090), timeout=10)

        for i in range(NUM_KEYS):
            request(sock, {"action": "PUT", "key": f"w{n}-k{i}", "value": f"v{n}-{i}"})

        for i in range(NUM_KEYS):
            reply = request(sock, {"action": "GET", "key": f"w{n}-k{i}"})
            assert reply.get("value") == f"v{n}-{i}", f"wrong value: {reply}"

        request(sock, {"action": "ENQUEUE", "item": f"task-{n}"})
        reply = request(sock, {"action": "DEQUEUE"})
        assert reply.get("status") == "OK", f"dequeue failed: {reply}"

        sock.close()
        print(f"{name} Transaction verified successfully.")
    except Exception as e:
        failures.append(n)
        print(f"{name} FAILED: {e}")


def main():
    print(f"[*] Launching {NUM_WORKERS} concurrent client worker threads...")
    start = time.time()

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(NUM_WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()   # wait for every thread to finish

    elapsed = time.time() - start
    if failures:
        print(f"[-] {len(failures)} worker(s) failed: {sorted(failures)}")
    else:
        print(f"[+] All {NUM_WORKERS} workers completed execution in {elapsed:.3f} seconds.")


if __name__ == "__main__":
    main()
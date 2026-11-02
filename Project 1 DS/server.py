
import socket
from concurrent.futures import ThreadPoolExecutor

from protocol import recv_message, send_message
from storage import KeyValueStore, TaskQueue

HOST = "0.0.0.0"
PORT = 9090
MAX_WORKERS = 10

store = KeyValueStore()
queue = TaskQueue()


def handle_request(req):
    """Do the requested action and return a reply dict."""
    action = req.get("action")

    if action == "PUT":
        store.put(req["key"], req["value"])
        return {"status": "OK"}

    if action == "GET":
        value = store.get(req["key"])
        if value is None:
            return {"status": "ERROR", "message": "key not found"}
        return {"status": "OK", "value": value}

    if action == "ENQUEUE":
        queue.enqueue(req["item"])
        return {"status": "OK"}

    if action == "DEQUEUE":
        item = queue.dequeue()
        if item is None:
            return {"status": "ERROR", "message": "queue empty"}
        return {"status": "OK", "item": item}

    return {"status": "ERROR", "message": "unknown action"}


def handle_client(conn):
    """Runs in a worker thread. Serve one client until it disconnects."""
    try:
        while True:
            req = recv_message(conn)
            try:
                reply = handle_request(req)
            except KeyError as e:          # request was missing a field
                reply = {"status": "ERROR", "message": f"missing field {e}"}
            send_message(conn, reply)
    except Exception:
        # Client disconnected or sent bad data. Drop just this client
        # the server and the other clients keep running
        pass
    finally:
        conn.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)   # allow quick restarts
    server.bind((HOST, PORT))
    server.listen()

    print(f"[*] Server initialized on {HOST}:{PORT}")
    print(f"[*] Thread Pool initialized with max_workers={MAX_WORKERS}...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        while True:
            conn, addr = server.accept()      # wait for a client to connect
            pool.submit(handle_client, conn)  # give it to a free worker


if __name__ == "__main__":
    main()
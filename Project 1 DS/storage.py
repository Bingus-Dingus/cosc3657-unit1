

import threading
from collections import deque


class KeyValueStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def put(self, key, value):
        with self.lock:            # only one thread at a time past this line
            self.data[key] = value

    def get(self, key):
        with self.lock:
            return self.data.get(key)   # None if the key isn't there


class TaskQueue:
    def __init__(self):
        self.items = deque()
        # A Condition is a lock that also lets threads sleep until
        # another thread wakes them up.
        self.cond = threading.Condition()

    def enqueue(self, item):
        with self.cond:
            self.items.append(item)
            self.cond.notify()     # wake up one thread waiting in dequeue()

    def dequeue(self, timeout=5):
        with self.cond:
            if not self.items:
                # Queue is empty: sleep until enqueue() calls notify(),
                # or give up after `timeout` seconds.
                self.cond.wait(timeout)
            if not self.items:
                return None        # still empty after waiting
            return self.items.popleft()
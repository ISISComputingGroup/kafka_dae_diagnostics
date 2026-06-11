import threading
from typing import Callable
from kafka_dae_diagnostics._kdaediag_rs import Data

class Callbacks:
    def __init__(self):
        self._lock = threading.RLock()
        self._callbacks = {}

    def add_callback(self, name, callback: Callable[[Data], None]):
        with self._lock:
            self._callbacks[name] = callback

    def remove_callback(self, name):
        with self._lock:
            del self._callbacks[name]

    def run_callbacks(self, data: Data):
        with self._lock:
            for callback in self._callbacks.values():
                callback(data)
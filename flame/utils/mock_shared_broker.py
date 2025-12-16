import threading


class SharedBroker:
    """A mock shared broker for testing purposes."""
    def __init__(self):
        self.message_broker = {}
        self.lock = threading.Lock()

    def set(self, key, value):
        """Set a value in the shared broker."""
        with self.lock:
            self.message_broker[key] = value

    def get(self, key, default=None):
        """Get a value from the shared broker."""
        with self.lock:
            return self.message_broker.get(key, default)

    def delete(self, key):
        """Delete a value from the shared broker."""
        with self.lock:
            if key in self.message_broker:
                del self.message_broker[key]

    def clear(self):
        """Clear all data from the shared broker."""
        with self.lock:
            self.message_broker.clear()
import time

class redis_controller:
    def __init__(self):
        self._store = {}

    def invoke_trigger(self, command: int, args):
        key = args[0]
        default_value = args[1] if len(args) > 1 else None
        expiry = args[2] if len(args) > 2 and args[2] else None
        now = int(time.time())

        if command == 1:  # get
            item = self._store.get(key)
            if not item:
                return default_value
            value, exp_at = item
            if exp_at and now > exp_at:
                del self._store[key]
                return default_value
            return value

        if command == 2:  # set
            exp_at = now + int(expiry) if expiry else None
            self._store[key] = (default_value, exp_at)
            return True

        return None

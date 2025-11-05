class RequestParser:
    def __init__(self, proxy=None, model=None, reset_cache=False):
        self.proxy = proxy
        self.model = model
        self.reset_cache = reset_cache

    def parse(self):
        print("[RequestParser] Starting parse() ...")
        if not self.model:
            print("[RequestParser] ❌ No model provided")
            return

        if self.reset_cache:
            print("[RequestParser] Resetting cache")
            self.model.reset_cache()

        print(f"[RequestParser] Setting proxy: {self.proxy}")
        self.model.set_proxy(self.proxy)

        print("[RequestParser] Running model.run() ...")
        result = self.model.run()
        print("[RequestParser] ✅ Finished. Result:")
        print(result)
        return result

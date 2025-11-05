import redis
import json

def get_all_keys(pattern):
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    keys = r.keys(pattern)
    print(f"Found {len(keys)} keys for pattern '{pattern}'")
    for k in keys:
        val = r.get(k)
        print("\n------------------------------")
        print(f"KEY: {k}")
        print("VALUE:")
        print(val)
    print("\n✅ Done fetching Redis data")

if __name__ == "__main__":
    # Show both raw and processed data
    get_all_keys("THN:*")
    get_all_keys("HACKREAD:*")
    get_all_keys("CSO:*")
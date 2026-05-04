import redis

r = redis.Redis(host="localhost", port=6379)

try:
    print("Redis response:", r.ping())
except Exception as e:
    print("Redis error:", e)
from faker import Faker
import random
from datetime import datetime
import os

fake = Faker()

OUTPUT_DIR = r"D:\data-engineering-project\data\raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_LEVELS = ["INFO", "WARN", "ERROR"]
ENDPOINTS = ["/login", "/logout", "/checkout", "/search", "/profile", "/payment"]

def generate_log_line():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = random.choice(LOG_LEVELS)
    ip = fake.ipv4()
    user_id = random.randint(1000, 9999)
    endpoint = random.choice(ENDPOINTS)
    status_code = random.choice([200, 200, 200, 400, 401, 403, 404, 500])
    response_time_ms = random.randint(20, 3000)

    log_line = (
        f"{timestamp} | {level} | ip={ip} | user_id={user_id} | "
        f"endpoint={endpoint} | status={status_code} | "
        f"response_time_ms={response_time_ms}"
    )

    return log_line

def main():
    file_name = f"app_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_path = os.path.join(OUTPUT_DIR, file_name)

    with open(file_path, "w") as f:
        for _ in range(1000):  # 1000 log lines
            f.write(generate_log_line() + "\n")

    print(f"Generated raw log file: {file_path}")

if __name__ == "__main__":
    main()

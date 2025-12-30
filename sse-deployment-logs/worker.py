import time
import random
from datetime import datetime
from faker import Faker
import redis
import json

fake = Faker()
r=redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def generate_log_line():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg_typ=random.choice(['INFO', 'WARNING', 'ERROR', 'DEBUG'])
    if msg_typ=='INFO':
        message = fake.sentence(nb_words=6)
    elif msg_typ=='WARNING':
        message = f"Potential issue detected in {fake.domain_name()}"
    else:
        message = f"Processing chunk {fake.hexify(text='^^^^')}"
    log_line = f"{timestamp} - {msg_typ} - {message}"
    return log_line

def run_build(job_id):
    channel = f"channel:{job_id}"
    list_key = f"logs:{job_id}" # we can add ttl to this key if needed
    total_lines = random.randint(10, 20)
    for i in range(total_lines):
        log_line = generate_log_line()
        msg={
            "id":i+1,
            "msg":log_line,
            "total_steps":total_lines # to track progress. if id equals total_steps, build is done
        }
        msg_json=json.dumps(msg)
        r.rpush(list_key, msg_json)
        r.publish(channel, msg_json)
        time.sleep(random.uniform(1, 3))  # Simulate time delay between log lines
    print(f"Build {job_id} completed. Total log lines: {total_lines}")

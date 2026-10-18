import os
import time

def generate_file(file_name):
    if os.path.exists(file_name):
        print(f"File '{file_name}' already exists. Skipping generation.")
        return
    target_size=1024*1024*1024/2 # 0.5 GB
    current_size=0
    with open(file_name, 'w') as f:
        while current_size < target_size:
            line = f"id_{current_size}:value_{current_size}\n"
            f.write(line)
            current_size += len(line)
    print(f"File '{file_name}' generated with size {target_size} bytes.")
    
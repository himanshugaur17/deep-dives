import os
import psutil
import time
import threading
import matplotlib.pyplot as plt

# --- Configuration ---
FILE_NAME = "physical_test_data.txt"
# 25MB file to prevent the test from taking hours
TARGET_SIZE = 25 * 1024 * 1024 
TARGET_LINE = 10000 

def generate_small_file():
    if os.path.exists(FILE_NAME):
        if os.path.getsize(FILE_NAME) >= TARGET_SIZE:
            print(f"File {FILE_NAME} already exists. Skipping generation.")
            return

    print(f"Generating 25MB test file: {FILE_NAME}...")
    with open(FILE_NAME, 'w') as f:
        current_size = 0
        i = 0
        while current_size < TARGET_SIZE:
            line = f"id_{i}:value_{i * 2}\n"
            f.write(line)
            current_size += len(line)
            i += 1
    print("Generation complete.")

class PerformanceMonitor:
    def __init__(self):
        self.running = False
        self.history = {
            "time": [],
            "disk_read_rate": [],
            "ram": [],
            "disk_write_rate": [],
            "iops": []
        }
        # Faster interval to catch quick operations
        self.interval = 0.05 
        self.start_time = time.time()
        self.thread = None
        self.process = psutil.Process(os.getpid())
        self.disk_start = psutil.disk_io_counters()
        self.last_check_time = time.time()

    def _capture(self):
        """Captures a single snapshot of metrics."""
        now = time.time()
        time_delta = now - self.last_check_time
        
        # Avoid division by zero if called too fast
        if time_delta < 0.001: 
            time_delta = 0.001
            
        current_elapsed = now - self.start_time

        # 1. RAM Metric
        ram_metric = self.process.memory_info().rss / (1024 * 1024)
        
        # 2. Disk Metrics
        disk_now = psutil.disk_io_counters()
        
        # Calculate Deltas
        read_bytes = disk_now.read_bytes - self.disk_start.read_bytes
        write_bytes = disk_now.write_bytes - self.disk_start.write_bytes
        read_ops = disk_now.read_count - self.disk_start.read_count
        write_ops = disk_now.write_count - self.disk_start.write_count
        
        # Calculate Rates
        iops = (read_ops + write_ops) / time_delta
        read_rate = (read_bytes / (1024*1024)) / time_delta
        write_rate = (write_bytes / (1024*1024)) / time_delta
        
        # Record
        self.history["time"].append(current_elapsed)
        self.history["ram"].append(ram_metric)
        self.history["disk_read_rate"].append(read_rate)
        self.history["disk_write_rate"].append(write_rate)
        self.history["iops"].append(iops)
        
        # Reset Baselines
        self.disk_start = disk_now
        self.last_check_time = now

    def _monitor_loop(self):
        while self.running:
            time.sleep(self.interval)
            self._capture()

    def start(self):
        self.running = True
        # Reset history
        self.history = {k: [] for k in self.history}
        self.start_time = time.time()
        self.last_check_time = time.time()
        self.disk_start = psutil.disk_io_counters()
        
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.start()
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        
        # FORCE one final capture to ensure we have data even if test was fast
        self._capture()
        return self.history

# --- EXPERIMENT 1: In-Memory (Physical Write at End) ---
def random_insertion_in_file_with_ram():
    start_time = time.time()
    print("Starting In-Memory Insertion (Physical Write)...")
    
    with open(FILE_NAME, 'r+') as f:
        # Read Phase 
        curr_line = 0
        while curr_line < TARGET_LINE:
            line = f.readline()
            if not line: break
            curr_line += 1
            
        target_pos = f.tell()
        remaining_lines = f.readlines() 
        
        # Write Phase
        f.seek(target_pos)
        f.write("New line inserted\n")
        f.writelines(remaining_lines)
        
        # --- THE PHYSICAL FORCE ---
        f.flush()            
        os.fsync(f.fileno()) 
        
    end_time = time.time()
    print(f"In-Memory Physical completed in {end_time - start_time:.2f} seconds.")

# --- EXPERIMENT 2: Streaming (Physical Write Every Line) ---
def random_insertion_in_file_wo_ram():
    start_time = time.time()
    print("Starting Streaming Insertion (Physical Write)...")
    
    temp_file = FILE_NAME + ".tmp"
    curr_line = 0
    
    with open(FILE_NAME, 'r') as f_in, open(temp_file, 'w') as f_out:
        for line in f_in:
            if curr_line == TARGET_LINE:
                f_out.write("New line inserted\n")
                f_out.flush()
                os.fsync(f_out.fileno())
                
            f_out.write(line)
            
            # --- THE PHYSICAL FORCE ---
            # Force disk write for EVERY line
            f_out.flush()
            os.fsync(f_out.fileno())
            
            curr_line += 1
            
    os.replace(temp_file, FILE_NAME)
    end_time = time.time()
    print(f"Streaming Physical completed in {end_time - start_time:.2f} seconds.")

def plot_results(data_a, data_b):
    print("Generating plot...")
    plt.style.use('dark_background') 
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(10, 16), sharex=False)
    
    # Helper to avoid empty plot errors
    if not data_a["time"] or not data_b["time"]:
        print("Warning: One of the datasets is empty! Test ran too fast?")
        return

    # 1. RAM
    ax1.plot(data_a["time"], data_a["ram"], label="In-Memory", color="#ff4444")
    ax1.plot(data_b["time"], data_b["ram"], label="Streaming", color="#44ff44")
    ax1.set_title("1. RAM Utilization (MB)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Read Rate
    ax2.plot(data_a["time"], data_a["disk_read_rate"], label="In-Memory", color="#ff8888", linestyle="--")
    ax2.plot(data_b["time"], data_b["disk_read_rate"], label="Streaming", color="#88ff88", linestyle="--")
    ax2.set_title("2. Disk Read Rate (MB/s)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # 3. Write Rate
    ax3.plot(data_a["time"], data_a["disk_write_rate"], label="In-Memory", color="#ff4444")
    ax3.plot(data_b["time"], data_b["disk_write_rate"], label="Streaming", color="#44ff44")
    ax3.set_title("3. Disk Write Rate (MB/s)")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # 4. IOPS
    ax4.plot(data_a["time"], data_a["iops"], label="In-Memory", color="orange")
    ax4.plot(data_b["time"], data_b["iops"], label="Streaming", color="cyan")
    ax4.set_title("4. IOPS (Disk Activity)")
    ax4.set_ylabel("Ops/Sec")
    ax4.set_xlabel("Time (seconds)")
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig("physical_io_benchmark.png")
    print("Plot saved to physical_io_benchmark.png")

if __name__ == "__main__":
    generate_small_file()

    # 1. Run In-Memory 
    monitor = PerformanceMonitor()
    monitor.start()
    random_insertion_in_file_with_ram()
    data_ram = monitor.stop()

    time.sleep(2)

    # 2. Run Streaming 
    monitor = PerformanceMonitor()
    monitor.start()
    random_insertion_in_file_wo_ram()
    data_stream = monitor.stop()

    plot_results(data_ram, data_stream)
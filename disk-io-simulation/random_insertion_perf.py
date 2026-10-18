import os
import psutil
import time
import threading
from generate_data import generate_file
import matplotlib.pyplot as plt

file_name = "data.txt"
target_line = 250000 

class PerformanceMonitor:  # Fixed spelling
    def __init__(self):
        self.running = False
        self.history = {
            "time": [],
            "disk_read_rate": [],
            "ram": [],
            "disk_write_rate": [],
            "iops": []
        }
        self.interval = 0.1
        self.start_time = time.time()
        self.thread = None

    def _monitor(self):
        process = psutil.Process(os.getpid())
        disk_start = psutil.disk_io_counters()
        last_check_time = time.time()
        
        while self.running:
            time.sleep(self.interval) # Sleep first to create a delta
            
            now = time.time()
            time_delta = now - last_check_time
            current_elapsed = now - self.start_time

            # 1. RAM Metric
            ram_metric = process.memory_info().rss / (1024 * 1024) # MB
            
            # 2. Disk Metrics
            disk_now = psutil.disk_io_counters()
            disk_read_bytes = disk_now.read_bytes - disk_start.read_bytes
            read_ops= disk_now.read_count - disk_start.read_count
            write_ops= disk_now.write_count - disk_start.write_count
            iops=(read_ops + write_ops) / time_delta
            disk_write_bytes = disk_now.write_bytes - disk_start.write_bytes

            disk_read_rate = (disk_read_bytes / (1024 * 1024)) / time_delta
            disk_write_rate = (disk_write_bytes / (1024 * 1024)) / time_delta
            
            # Update Baselines
            disk_start = disk_now
            last_check_time = now
            
            # Record
            self.history["time"].append(current_elapsed)
            self.history["ram"].append(ram_metric)
            self.history["disk_read_rate"].append(disk_read_rate)
            self.history["disk_write_rate"].append(disk_write_rate)
            self.history["iops"].append(iops)

    def start(self):
        self.running = True
        # Reset history
        self.history = {k: [] for k in self.history}
        self.thread = threading.Thread(target=self._monitor)
        self.thread.start()
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        return self.history

def random_insertion_in_file_with_ram():
    start_time = time.time()
    print("Starting In-Memory Insertion...")
    with open(file_name, 'r+') as f:
        curr_line = 0
        # Navigate to target
        while curr_line < target_line:
            line = f.readline()
            if not line: break
            curr_line += 1
            
        target_file_position = f.tell()
        
        # This loads the ENTIRE rest of the file into RAM
        remaining_lines = f.readlines() 
        
        f.seek(target_file_position) # pointer seeked back to target
        f.write("New line inserted\n")
        f.writelines(remaining_lines) # Write back all remaining lines
    end_time = time.time()
    print(f"In-Memory Insertion completed in {end_time - start_time:.2f} seconds.")

def random_insertion_in_file_wo_ram():
    start_time = time.time()
    print("Starting Streaming Insertion...")
    temp_file_name = "temp_data.txt"
    curr_line = 0
    with open(file_name, 'r') as f, open(temp_file_name, 'w') as temp_f:
        for line in f:
            if curr_line == target_line:
                temp_f.write("New line inserted\n")
            temp_f.write(line)
            curr_line += 1
    os.replace(temp_file_name, file_name)
    end_time = time.time()
    print(f"Streaming Insertion completed in {end_time - start_time:.2f} seconds.")

def plot_results(data_a, data_b):
    print("Generating comprehensive plot...")
    plt.style.use('dark_background') 
    
    # Create 4 subplots sharing the X axis (Time)
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(10, 16), sharex=False)
    
    # 1. RAM Utilization
    ax1.plot(data_a["time"], data_a["ram"], label="In-Memory", color="#ff4444", linewidth=2)
    ax1.plot(data_b["time"], data_b["ram"], label="Streaming", color="#44ff44", linewidth=2)
    ax1.set_title("1. RAM Utilization (MB)")
    ax1.set_ylabel("MB")
    ax1.legend(loc="upper right")
    ax1.grid(True, alpha=0.3)
    
    # 2. Disk Read Rate
    ax2.plot(data_a["time"], data_a["disk_read_rate"], label="In-Memory", color="#ff8888", linestyle="--")
    ax2.plot(data_b["time"], data_b["disk_read_rate"], label="Streaming", color="#88ff88", linestyle="--")
    ax2.set_title("2. Disk Read Rate (MB/s)")
    ax2.set_ylabel("MB/s")
    ax2.legend(loc="upper right")
    ax2.grid(True, alpha=0.3)

    # 3. Disk Write Rate
    ax3.plot(data_a["time"], data_a["disk_write_rate"], label="In-Memory", color="#ff4444")
    ax3.plot(data_b["time"], data_b["disk_write_rate"], label="Streaming", color="#44ff44")
    ax3.set_title("3. Disk Write Rate (MB/s)")
    ax3.set_ylabel("MB/s")
    ax3.legend(loc="upper right")
    ax3.grid(True, alpha=0.3)

    # 4. Disk IOPS (Utilization Proxy)
    ax4.plot(data_a["time"], data_a["iops"], label="In-Memory", color="orange", alpha=0.8)
    ax4.plot(data_b["time"], data_b["iops"], label="Streaming", color="cyan", alpha=0.8)
    ax4.set_title("4. Disk Activity (IOPS)")
    ax4.set_ylabel("Ops/Sec")
    ax4.set_xlabel("Time (seconds)")
    ax4.legend(loc="upper right")
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_file = "io_benchmark_full.png"
    plt.savefig(output_file)
    print(f"Plot saved to {output_file}")

if __name__=="__main__":
    generate_file(file_name)

    # 2. Test without RAM 
    monitor = PerformanceMonitor()
    monitor.start()
    random_insertion_in_file_wo_ram()
    history_wo_ram = monitor.stop()

    time.sleep(2) # Cooldown for disk

    # 1. Test RAM 
    monitor = PerformanceMonitor()
    monitor.start()
    random_insertion_in_file_with_ram()
    history_with_ram = monitor.stop()

    

    plot_results(history_with_ram, history_wo_ram)
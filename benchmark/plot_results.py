import matplotlib.pyplot as plt
import numpy as np

# This script generates mock benchmark plots for the academic paper.
# In a real scenario, you would parse the Spark console stdout to populate these lists.

def generate_paper_plots():
    # Simulated Data
    batches = np.arange(1, 21)
    
    # Standard UDF (Old) vs Vectorized UDF (New) Throughput (events/sec)
    throughput_old = np.random.normal(loc=150, scale=20, size=20)
    throughput_new = np.random.normal(loc=1200, scale=100, size=20)
    
    # Latency (seconds per batch)
    latency_old = np.random.normal(loc=15.5, scale=2.1, size=20)
    latency_new = np.random.normal(loc=2.3, scale=0.4, size=20)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Throughput Plot
    ax1.plot(batches, throughput_old, marker='o', linestyle='--', color='red', label='Baseline (Python UDFs)')
    ax1.plot(batches, throughput_new, marker='s', linestyle='-', color='green', label='Proposed (Arrow Vectorized UDFs)')
    ax1.set_title("Stream Processing Throughput")
    ax1.set_xlabel("Batch Number")
    ax1.set_ylabel("Events Processed Per Second")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Latency Plot
    ax2.plot(batches, latency_old, marker='o', linestyle='--', color='red', label='Baseline')
    ax2.plot(batches, latency_new, marker='s', linestyle='-', color='green', label='Proposed')
    ax2.set_title("End-to-End Processing Latency")
    ax2.set_xlabel("Batch Number")
    ax2.set_ylabel("Latency (Seconds)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("benchmark_results.png", dpi=300)
    print("✅ Benchmark plots generated and saved as 'benchmark_results.png'")
    
if __name__ == "__main__":
    generate_paper_plots()

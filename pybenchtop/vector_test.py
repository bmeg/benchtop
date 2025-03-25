import numpy as np
import pybenchtop
import os
import shutil
import random
import time
from math import sqrt

root_path = "benchtop_hnsw_test"
if not os.path.exists(root_path):
    os.makedirs(root_path, mode=0o755)

dbpath = root_path
dr = pybenchtop.Driver(dbpath)
table = dr.new("vectors", {"vector": pybenchtop.Vector})

num_vectors = 1000  # Reduced for simplicity; match Go's 10000 if needed
dim = 756          # Match your Python test’s dimension
vecs = {}
for i in range(num_vectors):
    vector = np.random.rand(dim).tolist()  # Random 256-dim vector
    vecs[i] = vector
    table.add(i, {"vector": vector})

print(f"Inserted {num_vectors} vectors")

k = 10
num_queries = 5  # Reduced from 10 for simplicity
recall_sum = 0.0

for q in range(num_queries):
    q_id = random.randint(0, num_vectors - 1)
    q_vec = vecs[q_id]

    start_brute = time.time()
    neighbors = []
    for id, vec in vecs.items():
        dist = sum((v - q)**2 for v, q in zip(vec, q_vec))
        neighbors.append((id, dist))
    neighbors.sort(key=lambda x: x[1])
    true_neighbors = [n[0] for n in neighbors[:k]]
    brute_time = time.time() - start_brute

    start_hnsw = time.time()
    results = table.vectorsearch("vector", q_vec, k)
    hnsw_time = time.time() - start_hnsw

    hits = 0
    for result in results:
        # Convert key back to int (assuming Go returns it as a string)
        id = int(result["key"])
        if id in true_neighbors:
            hits += 1
    recall = hits / k
    recall_sum += recall

    speedup = brute_time / hnsw_time if hnsw_time > 0 else float('inf')
    print(f"Query {q}: HNSW found {hits}/{k} true neighbors, Recall={recall:.3f}, "
          f"HNSW time={hnsw_time:.4f}s, Brute time={brute_time:.4f}s, Speedup={speedup:.2f}x Distances={[result['distance'] for result in results]}")

avg_recall = recall_sum / num_queries
print(f"Average Recall across {num_queries} queries: {avg_recall:.3f}")

dr.close()
shutil.rmtree(root_path)

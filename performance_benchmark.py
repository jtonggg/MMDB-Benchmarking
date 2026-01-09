import time
import statistics
import subprocess
import json
from faker import Faker
from pymongo import MongoClient
from neo4j import GraphDatabase
from arango import ArangoClient

# --- CONFIGURATION SECTION ---
# ############################################################################
# NOTE: Change the names below to match your own Docker container names.
# You can find your container names by running 'docker ps' in your terminal.
# ############################################################################
ARANGO_CONTAINER = "objective_dewdney"  # <-- Change this to your ArangoDB container name
MONGO_CONTAINER  = "mongodb"            # <-- Change this to your MongoDB container name
NEO4J_CONTAINER  = "neo4j"              # <-- Change this to your Neo4j container name

DATA_SIZES = [100, 1000, 5000] 
NUM_RUNS = 3                    
fake = Faker()
NEO4J_PASS = "testpass"         

# --- CPU/MEM Functions ---
def parse_percent(value):
    """Convert '12.34%' to float 12.34, or return None if invalid."""
    try:
        return float(value.strip('%'))
    except:
        return None

def get_container_stats(container_name):
    """Fetches real-time resource utilization via Docker CLI."""
    try:
        result = subprocess.run(
            ["docker", "stats", container_name, "--no-stream", "--format", "{{json .}}"],
            capture_output=True, text=True, check=True
        )
        stats = json.loads(result.stdout)
        cpu = parse_percent(stats["CPUPerc"])
        mem = parse_percent(stats["MemPerc"])
        return cpu, mem
    except Exception as e:
        print(f"Error getting stats for {container_name}: {e}")
        return None, None

# --- Data Generation ---
def generate_data(count):
    """Generates synthetic product records for tiered testing."""
    return [
        {
            "id": fake.uuid4(),
            "name": fake.word().title() + " " + fake.word().title(),
            "price": round(fake.random_number(digits=3) + fake.random.random(), 2),
            "category": fake.word().title()
        }
        for _ in range(count)
    ]

# --- Benchmark ---
def run_scalability_test():
    results = {"Arango": {}, "Polyglot": {}}

    # Setup connections
    arango_client = ArangoClient(hosts='http://localhost:8529')
    db = arango_client.db('_system', username='root', password='arangopass')
    if not db.has_collection('Products'): db.create_collection('Products')
    col = db.collection('Products')

    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["BenchmarkingDB"]["Products"]
    neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", NEO4J_PASS))

    for size in DATA_SIZES:
        print(f"\nTesting Scalability: {size} Records")
        arango_runs, arango_cpu, arango_mem = [], [], []
        poly_runs, mongo_cpu, mongo_mem, neo4j_cpu, neo4j_mem = [], [], [], [], []

        data = generate_data(size)

        for run in range(NUM_RUNS):
            try:
                # --- ArangoDB Run ---
                col.truncate()
                arango_data = [{k: v for k, v in doc.items() if k != "_id"} for doc in data]
                start = time.time()
                col.insert_many(arango_data)
                elapsed = time.time() - start
                arango_runs.append(elapsed)
                
                # Use configured container name
                cpu, mem = get_container_stats(ARANGO_CONTAINER)
                arango_cpu.append(cpu)
                arango_mem.append(mem)
                print(f"ArangoDB Run {run+1}: Latency={elapsed:.4f}s | CPU={cpu} | MEM={mem}")

                # --- Polyglot Run ---
                mongo_db.delete_many({})
                with neo4j_driver.session() as session:
                    session.run("MATCH (n) DETACH DELETE n")
                
                neo4j_data = [{k: v for k, v in doc.items() if k != "_id"} for doc in data]
                start = time.time()
                mongo_db.insert_many(data)
                with neo4j_driver.session() as session:
                    session.run("UNWIND $props AS map CREATE (p:Product) SET p = map", props=neo4j_data)
                elapsed = time.time() - start
                poly_runs.append(elapsed)
                
                # Use configured container names for Polyglot
                m_cpu, m_mem = get_container_stats(MONGO_CONTAINER)
                n_cpu, n_mem = get_container_stats(NEO4J_CONTAINER)
                mongo_cpu.append(m_cpu)
                mongo_mem.append(m_mem)
                neo4j_cpu.append(n_cpu)
                neo4j_mem.append(n_mem)
                print(f"Polyglot Run {run+1}: Latency={elapsed:.4f}s | Mongo CPU={m_cpu} MEM={m_mem} | Neo4j CPU={n_cpu} MEM={n_mem}")

            except Exception as e:
                print(f"Run {run+1} failed: {e}")

        # Record average times and stats for the current tier
        results["Arango"][size] = {
            "latency": statistics.mean(arango_runs),
            "cpu": statistics.mean([v for v in arango_cpu if v is not None]),
            "mem": statistics.mean([v for v in arango_mem if v is not None])
        }
        results["Polyglot"][size] = {
            "latency": statistics.mean(poly_runs),
            "mongo_cpu": statistics.mean([v for v in mongo_cpu if v is not None]),
            "mongo_mem": statistics.mean([v for v in mongo_mem if v is not None]),
            "neo4j_cpu": statistics.mean([v for v in neo4j_cpu if v is not None]),
            "neo4j_mem": statistics.mean([v for v in neo4j_mem if v is not None])
        }

    # --- Results Table Summary ---
    print("\n" + "="*70)
    print(f"{'Data Size':<12} | {'Arango Lat (s)':<14} | {'Arango CPU (%)':<14} | {'Arango MEM (%)':<14} | "
          f"{'Poly Lat (s)':<12} | {'Mongo CPU (%)':<12} | {'Mongo MEM (%)':<12} | {'Neo4j CPU (%)':<12} | {'Neo4j MEM (%)':<12}")
    print("-" * 70)
    for size in DATA_SIZES:
        a = results["Arango"][size]
        p = results["Polyglot"][size]
        print(f"{size:<12} | {a['latency']:<14.4f} | {a['cpu']:<14.2f} | {a['mem']:<14.2f} | "
              f"{p['latency']:<12.4f} | {p['mongo_cpu']:<12.2f} | {p['mongo_mem']:<12.2f} | {p['neo4j_cpu']:<12.2f} | {p['neo4j_mem']:<12.2f}")
    print("="*70)

    neo4j_driver.close()

if __name__ == "__main__":
    run_scalability_test()

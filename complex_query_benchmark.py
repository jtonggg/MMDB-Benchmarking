import time
import subprocess
import json
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

NUM_RUNS = 3
NEO4J_PASS = "testpass"

def parse_percent(value):
    """Convert '12.34%' to float 12.34, or return None if invalid."""
    try:
        return float(value.strip('%'))
    except:
        return None

def get_container_stats(container_name):
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

def run_complex_query_benchmark():
    # --- Setup Connections ---
    arango_client = ArangoClient(hosts='http://localhost:8529')
    db = arango_client.db('_system', username='root', password='arangopass')
    arango_col = db.collection('Products')
    
    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_col = mongo_client["BenchmarkingDB"]["Products"]
    
    neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", NEO4J_PASS))

    # --- Benchmark ArangoDB ---
    arango_times, arango_cpu_stats, arango_mem_stats = [], [], []
    print("\nArangoDB Run Times:")
    for i in range(NUM_RUNS):
        try:
            start = time.time()
            cursor = db.aql.execute("""
                FOR p IN Products
                    COLLECT cat = p.category
                    AGGREGATE avgPrice = AVERAGE(p.price)
                    RETURN {category: cat, avg_price: avgPrice}
            """)
            results = list(cursor)
            elapsed = time.time() - start
            arango_times.append(elapsed)
            
            # Use the variable defined in configuration
            cpu, mem = get_container_stats(ARANGO_CONTAINER)
            arango_cpu_stats.append(cpu)
            arango_mem_stats.append(mem)
            
            print(f"Run {i+1}: Latency={elapsed:.4f}s | CPU={cpu} | MEM={mem}")
        except Exception as e:
            print(f"ArangoDB Run {i+1} failed: {e}")

    # --- Benchmark Polyglot (Mongo + Neo4j) ---
    poly_times = []
    mongo_cpu_stats, mongo_mem_stats = [], []
    neo4j_cpu_stats, neo4j_mem_stats = [], []
    print("\nPolyglot Run Times:")
    for i in range(NUM_RUNS):
        try:
            start = time.time()
            # MongoDB aggregation
            pipeline = [{"$group": {"_id": "$category", "avgPrice": {"$avg": "$price"}}}]
            mongo_results = list(mongo_col.aggregate(pipeline))
            # Neo4j traversal
            with neo4j_driver.session() as session:
                session.run("""
                    MATCH (p:Product)
                    RETURN p.category AS category, COUNT(p) AS total
                """)
            elapsed = time.time() - start
            poly_times.append(elapsed)
            
            # Use the variables defined in configuration
            mongo_cpu, mongo_mem = get_container_stats(MONGO_CONTAINER)
            neo4j_cpu, neo4j_mem = get_container_stats(NEO4J_CONTAINER)
            
            mongo_cpu_stats.append(mongo_cpu)
            mongo_mem_stats.append(mongo_mem)
            neo4j_cpu_stats.append(neo4j_cpu)
            neo4j_mem_stats.append(neo4j_mem)
            
            print(f"Run {i+1}: Latency={elapsed:.4f}s | Mongo CPU={mongo_cpu} MEM={mongo_mem} | Neo4j CPU={neo4j_cpu} MEM={neo4j_mem}")
        except Exception as e:
            print(f"Polyglot Run {i+1} failed: {e}")

    # --- Averages ---
    def avg(lst):
        vals = [v for v in lst if v is not None]
        return sum(vals)/len(vals) if vals else None

    print("\nAverage Results:")
    print(f"ArangoDB Avg Latency: {sum(arango_times)/NUM_RUNS:.4f}s | Avg CPU={avg(arango_cpu_stats):.2f}% | Avg MEM={avg(arango_mem_stats):.2f}%")
    print(f"Polyglot Avg Latency: {sum(poly_times)/NUM_RUNS:.4f}s")
    print(f" - MongoDB Avg CPU={avg(mongo_cpu_stats):.2f}% Avg MEM={avg(mongo_mem_stats):.2f}%")
    print(f" - Neo4j Avg CPU={avg(neo4j_cpu_stats):.2f}% Avg MEM={avg(neo4j_mem_stats):.2f}%")
    
    neo4j_driver.close()

if __name__ == "__main__":
    run_complex_query_benchmark()

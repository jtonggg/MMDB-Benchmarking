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

# Paste a valid Product ID from your database here to test retrieval
TEST_ID = "paste-an-id-here" 
NEO4J_PASS = "testpass"

def get_container_stats(container_name):
    """Fetches real-time CPU and Memory stats from Docker Engine."""
    try:
        result = subprocess.run(
            ["docker", "stats", container_name, "--no-stream", "--format", "{{json .}}"],
            capture_output=True, text=True, check=True
        )
        stats = json.loads(result.stdout)
        cpu = stats["CPUPerc"]
        mem = stats["MemPerc"]
        return cpu, mem
    except Exception as e:
        print(f"Error getting stats for {container_name}: {e}")
        return "N/A", "N/A"

def run_query_benchmark():
    # --- 1. ArangoDB (Unified Engine Access) ---
    arango_client = ArangoClient(hosts='http://localhost:8529')
    db = arango_client.db('_system', username='root', password='arangopass')
    col = db.collection('Products')
    
    start = time.time()
    # Integrated query: direct access to document attributes
    db.aql.execute("FOR p IN Products FILTER p.id == @id RETURN p", bind_vars={'id': TEST_ID})
    arango_q_time = time.time() - start
    
    # Use the configuration variable for Arango stats
    arango_cpu, arango_mem = get_container_stats(ARANGO_CONTAINER)

    # --- 2. Polyglot (Multi-Step Database Hop) ---
    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["BenchmarkingDB"]["Products"]
    neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", NEO4J_PASS))

    start = time.time()
    # Step A: Neo4j Hop - finding the reference in the graph
    with neo4j_driver.session() as session:
        session.run("MATCH (p:Product {id: $id}) RETURN p", id=TEST_ID)
    # Step B: MongoDB Hop - retrieving the document details
    mongo_db.find_one({"id": TEST_ID})
    poly_q_time = time.time() - start

    # Use the configuration variables for Polyglot stats
    mongo_cpu, mongo_mem = get_container_stats(MONGO_CONTAINER)
    neo4j_cpu, neo4j_mem = get_container_stats(NEO4J_CONTAINER)

    print(f"\nResults for SINGLE QUERY POINT RETRIEVAL:")
    print(f"ArangoDB: Latency={arango_q_time:.6f}s | CPU={arango_cpu} | MEM={arango_mem}")
    print(f"Polyglot: Latency={poly_q_time:.6f}s")
    print(f" - MongoDB CPU={mongo_cpu} MEM={mongo_mem}")
    print(f" - Neo4j CPU={neo4j_cpu} MEM={neo4j_mem}")

    neo4j_driver.close()

if __name__ == "__main__":
    run_query_benchmark()

import time
import subprocess
import json
from threading import Thread
from pymongo import MongoClient
from neo4j import GraphDatabase
from arango import ArangoClient
from faker import Faker

NUM_THREADS = 5
NUM_RECORDS_PER_THREAD = 200
NEO4J_PASS = "testpass"
fake = Faker()

def generate_data(count):
    return [
        {"id": fake.uuid4(), "name": fake.word().title(), "price": round(fake.random_number(digits=3) + fake.random.random(),2), "category": fake.word().title()}
        for _ in range(count)
    ]

def get_container_stats(container_name):
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

def arango_insert_task(col, data):
    col.insert_many(data)

def polyglot_insert_task(mongo_col, neo_driver, data):
    mongo_col.insert_many(data)
    neo_data = []
    for doc in data:
        clean_doc = doc.copy()
        if "_id" in clean_doc:
            clean_doc["_id"] = str(clean_doc["_id"])
        neo_data.append(clean_doc)
    with neo_driver.session() as session:
        session.run(
            "UNWIND $props AS map CREATE (p:Product) SET p = map",
            props=neo_data
        )

def run_concurrent_benchmark():
    # --- Setup connections ---
    arango_client = ArangoClient(hosts='http://localhost:8529')
    db = arango_client.db('_system', username='root', password='arangopass')
    arango_col = db.collection('Products')
    arango_col.truncate()

    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_col = mongo_client["BenchmarkingDB"]["Products"]
    mongo_col.delete_many({})

    neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", NEO4J_PASS))
    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    thread_data = [generate_data(NUM_RECORDS_PER_THREAD) for _ in range(NUM_THREADS)]

    # --- ArangoDB Insert ---
    threads = [Thread(target=arango_insert_task, args=(arango_col, data)) for data in thread_data]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    arango_time = time.time() - start
    arango_cpu, arango_mem = get_container_stats("objective_dewdney")  # Replace with your Arango container name

    # --- Polyglot Insert ---
    threads = [Thread(target=polyglot_insert_task, args=(mongo_col, neo4j_driver, data)) for data in thread_data]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    poly_time = time.time() - start
    mongo_cpu, mongo_mem = get_container_stats("mongodb")  # Replace with your Mongo container name
    neo4j_cpu, neo4j_mem = get_container_stats("neo4j")    # Replace with your Neo4j container name

    # --- Results ---
    print(f"\nConcurrent Inserts Benchmark:")
    print(f"ArangoDB: Latency={arango_time:.4f}s | CPU={arango_cpu} | MEM={arango_mem}")
    print(f"Polyglot: Latency={poly_time:.4f}s")
    print(f" - MongoDB CPU={mongo_cpu} MEM={mongo_mem}")
    print(f" - Neo4j CPU={neo4j_cpu} MEM={neo4j_mem}")

    neo4j_driver.close()

if __name__ == "__main__":
    run_concurrent_benchmark()

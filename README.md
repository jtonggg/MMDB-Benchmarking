# MMDB-Benchmarking
This repository provides the experimental framework and Python benchmarking scripts used to compare the architectural efficiency of a Native Multi-Model Database (ArangoDB) against a Polyglot Persistence stack (MongoDB + Neo4j).

# Project Overview
The study evaluates these architectures under controlled workloads to identify trade-offs in performance, resource consumption, and scalability. The benchmarks focus on:
- Ingestion latency
- Query response time
- Resource utilization (CPU & memory)
- Scalability across dataset sizes （100, 1000 and 5000 records）

Databases tested:
ArangoDB (native multi-model)
MongoDB + Neo4j (polyglot persistence)

# Prerequisites
Before running the scripts, ensure you have the following installed:
- Docker Desktop – to run containerized databases
- Python 3.12 – for running benchmarking scripts
- Python Libraries – install via pip:

pip install faker pymongo neo4j python-arango

- Visual Studio Code (optional) – recommended for running/debugging scripts.


# Setup Instructions
1. Clone this repository:
2. To initialize Database Containers，run the following commands to start the specific NoSQL engines used in this study :
   
ArangoDB

docker run -d --name arangodb -p 8529:8529 -e ARANGO_ROOT_PASSWORD=arangopass arangodb:3.12.7

MongoDB

docker run -d --name mongodb -p 27017:27017 mongo:8.2.3

Neo4j

docker run -d --name neo4j -p 7687:7687 -e NEO4J_AUTH=neo4j/testpass neo4j:2025.11.2

3. Verify containers are running:
docker ps

# Running the Benchmarks
Important: Each script contains a Configuration Section at the top. Ensure the container names in the script match your docker ps output before running.


# Methodology Highlights
Data Generation: Synthetic enterprise product catalogs generated via the Faker library.

Resource Tracking: Performance metrics are captured directly from the Docker Engine kernel via a custom monitoring wrapper.

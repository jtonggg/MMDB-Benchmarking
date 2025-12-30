# MMDB-Benchmarking
Python scripts for Multi-Model Database benchmarking study comparing ArangoDB and a Polyglot stack (MongoDB + Neo4j).

# Project Overview
This repository contains scripts, datasets, and instructions to reproduce the performance benchmarks for evaluating:
- Ingestion latency
- Query response time
- Resource utilization (CPU & memory)
- Scalability across dataset sizes

Databases tested:
ArangoDB (native multi-model)
MongoDB + Neo4j (polyglot persistence)

# Prerequisites
Before running the scripts, ensure you have the following installed:
Docker Desktop – to run containerized databases
Python 3.12 – for running benchmarking scripts
Python Libraries – install via pip:
pip install faker pymongo neo4j python-arango
Visual Studio Code (optional) – recommended for running/debugging scripts.


# Setup Instructions
1. Clone this repository:
2. Start the database containers using Docker:
docker run -d --name arangodb -p 8529:8529 arangodb:3.12.7
docker run -d --name mongodb -p 27017:27017 mongo:8.2.3
docker run -d --name neo4j -p 7687:7687 neo4j:2025.11.2
4. Verify containers are running:
docker ps




# Lucene State Store for Kafka Streams

A proof of concept demonstrating Lucene embedded as a state store engine for Apache Kafka Streams applications.

## Overview

This repository explores innovative approaches to state management in Kafka Streams by replacing the default RocksDB state store with Lucene. This enables leveraging full-text search capabilities directly within Kafka Streams applications for complex state queries and lookups.

## Key Features

- **Full-Text Search State Management**: Utilize Lucene's powerful full-text search capabilities for state queries
- **Embedded Mode**: Lucene runs embedded within your Kafka Streams application
- **Alternative State Store**: Demonstrates moving beyond RocksDB for specialized use cases
- **Event-driven Architecture**: Combines the power of event streams with advanced search semantics

## Conference Talk

This project accompanies the conference talk [**"Kafka - No Rocks, Please: Using Kafka Streams with Alternative State Stores"**](https://current.confluent.io/2024-sessions/kafka---no-rocks-please-using-kafka-streams-with-alternative-state-stores) presented at Confluent Current 2024.

The talk explores why and when you might want to use alternative state stores instead of RocksDB, with live demonstrations and best practices for production deployments.

## Sister Repository

See also [@rkolesnev/kstreams-neo4j-statestore](https://github.com/rkolesnev/kstreams-neo4j-statestore) - another alternative state store implementation showcasing Neo4j for graph-based state management within Kafka Streams.

## Use Case: Taxi Hauling Lookup

This implementation demonstrates a practical use case: efficient geospatial and text-based taxi lookup using Lucene's search capabilities. The demo includes:

- Real-time taxi location tracking
- Geospatial queries for nearby taxi availability
- Full-text search on driver and vehicle information
- Interactive web interface for taxi monitoring and dispatch

## Technology Stack

- **Language**: Java
- **Kafka Streams**: Event streaming library
- **Lucene**: Embedded full-text search engine
- **Python**: Demo applications and utilities
- **Docker**: Confluent Platform deployment
- **License**: MIT

## Getting Started

### Prerequisites

- Java 16+
- Python 3.7+
- Docker and Docker Compose

### Running the Demo

See the [demo/README.md](./demo/README.md) for detailed instructions on building and running the taxi hauling lookup application.

### Attributions 

Originally developed with Niamh Thornbury and Ian Feeney and presented at Confluent Current 2022 talk [**"Real-Time Processing of Spatial Data Using Kafka Streams"**](https://www.confluent.io/events/current/2022/real-time-processing-of-spatial-data-using-kafka-streams/).

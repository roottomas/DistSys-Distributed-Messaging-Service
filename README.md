# SD-Trab2 — Distributed Messaging Service

A distributed messaging service developed for the **Distributed Systems** course. This project implements a secure, replicated messaging platform exposing both **REST** and **gRPC** APIs, with support for service discovery, Kafka-based replication, TLS, and persistent storage using Hibernate.

## Overview

The goal of this project is to explore the design and implementation of a distributed service capable of handling messaging operations while addressing common distributed systems challenges such as:

* Service communication through multiple protocols
* Data persistence
* Replication between servers
* Secure communication using TLS
* Service discovery
* Concurrent request handling

The project follows a modular architecture where clients communicate with replicated servers through REST or gRPC interfaces while updates are propagated across replicas using Kafka.

---

## Features

* REST API
* gRPC API
* Secure communication (TLS)
* Kafka-based replication
* Hibernate persistence
* Service discovery support
* Docker deployment
* Client libraries for REST and gRPC
* Mail proxy integration

---

## Project Architecture

```text
                   +----------------+
                   | REST Client    |
                   +----------------+
                           |
                           |
                   +----------------+
                   | gRPC Client    |
                   +----------------+
                           |
          ---------------------------------------
                          |
                  +------------------+
                  | Message Server   |
                  +------------------+
                    |            |
             Hibernate      Kafka Publisher
                    |            |
                 Database   Kafka Topic
                                 |
                          Kafka Subscriber
                                 |
                      Other Server Replicas
```

---

## Project Structure

```text
src/
 └── sd2526/
      └── trab/
           ├── api/
           │    ├── clients/
           │    ├── grpc/
           │    ├── rest/
           │    ├── server/
           │    │     ├── grpc/
           │    │     ├── rest/
           │    │     ├── java/
           │    │     ├── replication/
           │    │     ├── proxy/
           │    │     └── common/
           │    └── java/
           └── ...
```

Main components:

* **REST Server** – HTTP interface for client applications.
* **gRPC Server** – High-performance RPC interface.
* **Client Libraries** – Java clients supporting both REST and gRPC.
* **Replication Module** – Synchronizes updates using Apache Kafka.
* **Persistence Layer** – Hibernate ORM for database storage.
* **Proxy Layer** – External service integration (mail support).
* **TLS Utilities** – Secure communication using Java keystores.

---

## Building

Compile the project with Maven:

```bash
mvn clean package
```

The build generates an executable JAR that can be deployed directly or inside Docker.

---

## Running with Docker

Build the Docker image:

```bash
docker build -t sd-trab2 .
```

Run the container:

```bash
docker run sd-trab2
```

The Docker image includes:

* application JAR
* Hibernate configuration
* TLS certificates
* application properties

---

## Security

The project uses TLS for secure communication between clients and servers.

Included certificates include:

* client truststore
* server keystores
* helper scripts for generating certificates

---

## Replication

Server replicas synchronize updates through Apache Kafka.

The replication layer is responsible for:

* publishing state changes
* receiving updates from peer replicas
* maintaining eventual consistency across servers

---

## Persistence

Persistent storage is handled through Hibernate.

Configuration is provided in:

```
hibernate.cfg.xml
```

This allows application data to survive server restarts while keeping the persistence layer independent from the service logic.

---

## APIs

The project exposes two communication interfaces:

### REST

Suitable for web clients and HTTP integrations.

### gRPC

Designed for efficient binary communication between distributed services.

Both APIs provide equivalent functionality while demonstrating different distributed communication paradigms.

---

## Educational Goals

This project demonstrates several important Distributed Systems concepts:

* Remote communication
* Service-oriented architecture
* Replication
* Event-driven synchronization
* Secure distributed communication
* Data persistence
* Modular API design
* Docker deployment

---

## Authors

Tomás Silvestre and Ricardo Laur.

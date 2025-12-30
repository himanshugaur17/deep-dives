# SSE Deployment Logs Prototype

A real-time deployment log streaming system using **Server-Sent Events (SSE)**, **Redis Pub/Sub**, and **FastAPI**.

## 🏗 Architecture
This prototype solves the "Late Joiner" problem in distributed systems using a hybrid approach:
1.  **Redis List:** Persists log history (for users joining late).
2.  **Redis Pub/Sub:** Broadcasts live logs (for real-time updates).
3.  **FastAPI (Async):** Handles high-concurrency SSE connections without blocking.

## 🚀 Quick Start

### 1. Prerequisites
* [uv](https://github.com/astral-sh/uv) (Python package manager)
* Docker (for Redis)

### 2. Start Infrastructure
Start the Redis container:
```bash
docker compose up -d
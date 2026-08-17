# ma7-MQ Message Broker

A lightweight, production-ready message broker built with C# (.NET 8) and styled with Astro dashboard console UI.

## Features
*   **Astro Console:** Live metrics throughput dashboard and API message dispatcher.
*   **Smart Payload Compression:** GZip compression applied selectively based on Shannon entropy analysis.
*   **Resiliency Architecture:** Built-in Token Bucket Rate Limiter, Circuit Breaker, Exponential Backoff with Jitter, and Dead Letter Queue (DLQ).
*   **Observability:** Prometheus integration.

## Getting Started

### 1. Build and Run Server (Docker Compose)
To start both Redis and the C# API server:
```bash
docker-compose up --build
```

### 2. Run Astro Dashboard Console UI
Navigate to the console directory, install dependencies, and run:
```bash
cd console
npm install
npm run dev
```
Open `http://localhost:4321` in your browser to view the console.

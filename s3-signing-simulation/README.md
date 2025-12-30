# Mock S3 Presigned URL Prototype

This project simulates how AWS S3 generates and validates Presigned URLs. It acts as both the "Signer" (AWS SDK) and the "Validator" (S3 Service) to demonstrate secure, time-limited, and tamper-proof file uploads without using actual AWS infrastructure.

## Features

* **Custom Signature Generation:** Implements HMAC-SHA256 signing manually.
* **Time-Limited Access:** URLs automatically expire after a set duration (2 minutes).
* **Data Integrity:** Enforces `x-file-checksum` headers to detect data tampering during upload.
* **Zero Dependencies:** Uses standard Python libraries (except FastAPI/Uvicorn for the server).

## Prerequisites

* Python 3.9+
* `uv` (or `pip`)

## Installation

```bash
# Initialize project and install dependencies
uv init
uv add fastapi uvicorn
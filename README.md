# Project Setup Guide

This document outlines the essential steps to get this project up and running. It includes instructions for:

- Running PostgreSQL using Docker
- Installing Python 3 dependencies
- Additional notes for local development

---

## Step 1: Run PostgreSQL with Docker

Start a PostgreSQL instance using Docker:

docker run --name poc-postgres \
  -e POSTGRES_PASSWORD=<setpassword> \
  -p 5432:5432 \
  -d postgres

- Replace `<setpassword>` with your desired password.
- The database will be accessible on port `5432` on your local machine.

## Step 2: Set Up Python Environment

Make sure you have **Python 3.8+** installed.

Create and activate a virtual environment:

python3 -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

Install required dependencies:

pip install --upgrade pip
pip install requirements.txt

---
# Step 3: API Startup & Documentation

---

### Starting the API

To run the backend API locally:

```bash
python server.py
```

Make sure:
- PostgreSQL is running (e.g. via Docker)
- Required Python dependencies are installed

---

### 📘 API Documentation (Postman)

All API endpoints are documented in a Postman collection located at:

```bash
documentation/grower-poc.postman_collection.json
```

## 🚀 Step 4: Run the Script

Before running, make sure you export your Claude API key:

export ANTHROPIC_API_KEY="sk-ant-..."  # Linux/macOS
set ANTHROPIC_API_KEY=sk-ant-...       # Windows

Then run:

python claude_test.py

---

## 📝 Notes

- **Claude prompt file**: Make sure `prompt.txt` exists in the same directory.
- **Token safety**: This script prevents infinite loops by using a `try-except` block and has `max_tokens` set to 1024.

---

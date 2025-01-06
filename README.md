# Sierra Chart to Arctic ETL

This repository shows how to parse Sierra Chart's `.scid` (Time & Sales) and `.depth` (Market Depth) data, then load it **directly** into Arctic (a time-series library on top of MongoDB).

## 1) Requirements

- **Python** (3.8+ recommended)
- **MongoDB** (for Arctic)
- Python packages listed in `requirements.txt`

## 2) Setting up MongoDB + Arctic

**Option A: Install MongoDB Locally**

1. [Download and install MongoDB Community Edition](https://www.mongodb.com/docs/manual/administration/install-community/).
2. Start `mongod` service. For example (on Linux/macOS):
   ```bash
   mkdir -p /data/db
   mongod --dbpath /data/db --port 27017
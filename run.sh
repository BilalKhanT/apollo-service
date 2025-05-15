
#!/bin/bash

# Create required directories
mkdir -p apollo_data/crawl
mkdir -p apollo_data/process
mkdir -p apollo_data/clusters
mkdir -p apollo_data/scraped
mkdir -p apollo_data/downloads
mkdir -p apollo_data/metadata

# Start the FastAPI application
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
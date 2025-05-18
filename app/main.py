from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

# Import route modules
from app.api.routes import crawler, clusters, scraper, logs, health

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Apollo Web Crawler API",
    description="API for the Apollo Web Scrapper.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(crawler.router)
app.include_router(clusters.router)
app.include_router(scraper.router)
app.include_router(logs.router)
app.include_router(health.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from app.api.routes import crawl, cluster, scrape, logs
from app.utils.database import connect_to_mongo, close_mongo_connection

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Apollo Web Crawler API...")
    try:
        await connect_to_mongo()
        logger.info("Database connection established successfully")
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
    
    yield

    logger.info("Shutting down Apollo Web Crawler API...")
    try:
        await close_mongo_connection()
        logger.info("Database connection closed successfully")
    except Exception as e:
        logger.error(f"Error closing database connection: {str(e)}")

app = FastAPI(
    title="Apollo Web Crawler API",
    description="API for the Apollo Web Scrapper with Database Integration.",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include all routers
app.include_router(crawl.router)
app.include_router(cluster.router)
app.include_router(scrape.router)
app.include_router(logs.router)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        from app.utils.database import db
        if db.client:
            await db.client.admin.command('ping')
            db_status = "connected"
        else:
            db_status = "disconnected"
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        db_status = "error"
    
    return {
        "status": "healthy",
        "database": db_status,
        "version": "2.0.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
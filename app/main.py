from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from app.api.routes import crawl, cluster, scrape, logs, schedule
from app.utils.database import connect_to_mongo, close_mongo_connection
from app.services.schedule_service import schedule_service

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

        await schedule_service.start()
        logger.info("Schedule service started successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {str(e)}")
    
    yield

    logger.info("Shutting down Apollo Web Crawler API...")
    try:
        await schedule_service.stop()
        logger.info("Schedule service stopped successfully")

        await close_mongo_connection()
        logger.info("Database connection closed successfully")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")

app = FastAPI(
    title="Apollo Web Crawler API",
    description="Apollo Web Scrapper with Scheduled Crawling",
    version="2.1.0", 
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(crawl.router)
app.include_router(cluster.router)
app.include_router(scrape.router)
app.include_router(logs.router)
app.include_router(schedule.router) 

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

    schedule_status = schedule_service.get_status()
    
    return {
        "status": "healthy",
        "database": db_status,
        "schedule_service": schedule_status,
        "version": "2.1.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
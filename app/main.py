import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from app.api.routes import crawl, cluster, scrape, logs, schedule, restaurant_deal
from app.utils.database import connect_to_mongo, close_mongo_connection
from app.services.schedule_service import schedule_service
from app.utils.socket_manager import socket_manager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Apollo Web Crawler API with WebSocket support...")
    try:
        await connect_to_mongo()
        logger.info("Database connection established successfully")
        await schedule_service.start()
        logger.info("Schedule service started successfully")
        try:
            from app.utils.realtime_publisher import realtime_publisher
            await realtime_publisher.start()
            logger.info("Realtime publisher service started successfully")
        except Exception as e:
            logger.warning(f"Failed to start realtime publisher: {str(e)}")
        
        logger.info("WebSocket manager initialized successfully")
        await socket_manager.broadcast_server_message(
            "Apollo server started and ready to accept connections", 
            "info"
        )
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {str(e)}")
        raise
    
    yield

    logger.info("Shutting down Apollo Web Crawler API...")
    try:
        await socket_manager.broadcast_server_message(
            "Apollo server is shutting down", 
            "warning"
        )

        await schedule_service.stop()
        logger.info("Schedule service stopped successfully")

        await close_mongo_connection()
        logger.info("Database connection closed successfully")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")

app = FastAPI(
    title="Apollo Web Crawler API",
    description="Apollo Web Scraper with Real-time WebSocket Updates, Scheduled Crawling, and Restaurant Deals Scraping",
    version="2.3.0",  # Updated version to reflect new restaurant functionality
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include all route modules
app.include_router(crawl.router)
app.include_router(cluster.router)
app.include_router(scrape.router)
app.include_router(logs.router)
app.include_router(schedule.router)
app.include_router(restaurant_deal.router)  # Add the new restaurant routes

socket_app = socketio.ASGIApp(
    socket_manager.sio,
    other_asgi_app=app,
    socketio_path="/socket.io"
)

@app.get("/health")
async def health_check():
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

    try:
        websocket_stats = socket_manager.get_stats()
        websocket_status = "healthy"
    except Exception as e:
        logger.error(f"WebSocket health check failed: {str(e)}")
        websocket_stats = {}
        websocket_status = "error"
    
    # Add restaurant service health check
    try:
        from app.controllers.restaurant_result_controller import RestaurantResultController
        restaurant_stats = await RestaurantResultController.get_restaurant_statistics()
        restaurant_status = "healthy"
    except Exception as e:
        logger.error(f"Restaurant service health check failed: {str(e)}")
        restaurant_stats = {}
        restaurant_status = "error"
    
    return {
        "status": "healthy",
        "database": {
            "status": db_status
        },
        "schedule_service": schedule_status,
        "websocket": {
            "status": websocket_status,
            "stats": websocket_stats
        },
        "restaurant_service": {
            "status": restaurant_status,
            "stats": restaurant_stats
        },
        "version": "2.3.0",
        "services": [
            "web_crawling", 
            "content_scraping", 
            "file_downloading", 
            "scheduled_crawling", 
            "restaurant_deals_scraping"
        ]
    }

app = socket_app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0", 
        port=8000,
        reload=True,
        reload_dirs=["app"],
        log_level="info"
    )
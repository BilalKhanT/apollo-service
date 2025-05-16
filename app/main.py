from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional
import logging
import socketio
import asyncio
import json

#push

from app.models import (
    CrawlRequest, CrawlStatus, ClusterResult, YearCluster,
    ScrapingRequest, ScrapingStatus
)

from app.models.log_response import LogResponse
from app.utils.task_manager import task_manager
from app.utils.orchestrator import orchestrator
from app.utils.redis_client import RedisClient
from app.utils.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI application
app = FastAPI(
    title="Apollo Web Crawler API",
    description="API for the Apollo Web Crawler, which crawls websites, categorizes links, and scrapes content.",
    version="1.0.0"
)

# Create Socket.IO server
sio = socketio.AsyncServer(
    async_mode='asgi', 
    cors_allowed_origins='*',
)
socket_app = socketio.ASGIApp(sio)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins in development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the Socket.IO app
app.mount('/ws', socket_app)

# Socket.IO event handlers
@sio.event
async def connect(sid, environ):
    """Handle client connection."""
    logger.info(f"Client connected: {sid}")
    await sio.emit('connection_success', {'message': 'Connected successfully'}, room=sid)

@sio.event
async def disconnect(sid):
    """Handle client disconnection."""
    logger.info(f"Client disconnected: {sid}")

@sio.event
async def subscribe_task(sid, data):
    """Subscribe to updates for a specific task."""
    task_id = data.get('task_id')
    if not task_id:
        await sio.emit('error', {'message': 'Missing task_id parameter'}, room=sid)
        return
    
    # Check if task exists
    task = task_manager.get_task_status(task_id)
    if not task:
        await sio.emit('error', {'message': f'Task {task_id} not found'}, room=sid)
        return
    
    # Join task-specific room
    sio.enter_room(sid, f'task:{task_id}')
    sio.enter_room(sid, f'logs:{task_id}')
    
    # Send current task status
    if task['type'] == 'crawl':
        crawl_results = task.get('result', {}).get('crawl_results', {})
        current_status = {
            'id': task_id,
            'type': task['type'],
            'status': task['status'],
            'progress': task['progress'],
            'current_stage': task['status'],
            'links_found': crawl_results.get('total_links_found', 0),
            'pages_scraped': crawl_results.get('total_pages_scraped', 0),
            'error': task.get('error'),
            'clusters_ready': task.get('result', {}).get('cluster_complete', False) and 
                            task.get('result', {}).get('year_extraction_complete', False)
        }
    elif task['type'] == 'scrape':
        scrape_results = task.get('result', {}).get('scrape_results', {})
        download_results = task.get('result', {}).get('download_results', {})
        current_status = {
            'id': task_id,
            'type': task['type'],
            'status': task['status'],
            'progress': task['progress'],
            'pages_scraped': scrape_results.get('pages_scraped', 0),
            'files_downloaded': download_results.get('files_downloaded', 0),
            'error': task.get('error')
        }
    else:
        current_status = {
            'id': task_id,
            'type': task['type'],
            'status': task['status'],
            'progress': task['progress'],
            'error': task.get('error')
        }
    
    await sio.emit('task_update', current_status, room=sid)
    await sio.emit('message', {'text': f'Subscribed to task {task_id}'}, room=sid)
    
    logger.info(f"Client {sid} subscribed to task {task_id}")

# Routes for crawling
@app.post("/api/crawl", response_model=CrawlStatus, tags=["Crawling"])
async def start_crawl(
    request: CrawlRequest, 
    background_tasks: BackgroundTasks
):
    """
    Start a new crawl with the specified parameters.
    
    Args:
        request: Crawl request parameters
    
    Returns:
        Crawl status with task ID
    """
    # Create a new task
    task_id = task_manager.create_task(
        task_type="crawl",
        params=request.dict()
    )
    
    # Start the crawl in the background
    background_tasks.add_task(
        orchestrator.run_crawl,
        task_id=task_id,
        base_url=request.base_url,
        max_links_to_scrape=request.max_links_to_scrape,
        max_pages_to_scrape=request.max_pages_to_scrape,
        depth_limit=request.depth_limit,
        domain_restriction=request.domain_restriction,
        scrape_pdfs_and_xls=request.scrape_pdfs_and_xls,
        stop_scraper=request.stop_scraper
    )
    
    # Return the task status
    task_status = task_manager.get_task_status(task_id)
    
    return CrawlStatus(
        id=task_id,
        status=task_status["status"],
        progress=task_status["progress"],
        current_stage=task_status["status"],
        links_found=0,
        pages_scraped=0,
        error=task_status.get("error"),
        clusters_ready=False
    )

@app.get("/api/crawl/{task_id}", response_model=CrawlStatus, tags=["Crawling"])
async def get_crawl_status(task_id: str):
    """
    Get the status of a crawl task.
    
    Args:
        task_id: ID of the task
    
    Returns:
        Crawl status
    """
    # Get the task status
    task_status = task_manager.get_task_status(task_id)
    
    if not task_status:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    # Extract relevant information
    result = task_status.get("result") or {}
    
    # Check if the task is a crawl task
    if task_status["type"] != "crawl":
        raise HTTPException(status_code=400, detail=f"Task {task_id} is not a crawl task")
    
    # Get crawler-specific information
    crawl_results = result.get("crawl_results", {})
    links_found = crawl_results.get("total_links_found", 0)
    pages_scraped = crawl_results.get("total_pages_scraped", 0)
    
    # Check if clusters are ready
    clusters_ready = (
        result.get("cluster_complete", False) and
        result.get("year_extraction_complete", False)
    )
    
    # Determine current stage
    current_stage = task_status["status"]
    
    return CrawlStatus(
        id=task_id,
        status=task_status["status"],
        progress=task_status["progress"],
        current_stage=current_stage,
        links_found=links_found,
        pages_scraped=pages_scraped,
        error=task_status.get("error"),
        clusters_ready=clusters_ready
    )

@app.get("/api/crawl", response_model=List[CrawlStatus], tags=["Crawling"])
async def list_crawl_tasks(
    limit: int = Query(10, description="Maximum number of tasks to return")
):
    """
    List recent crawl tasks.
    
    Args:
        limit: Maximum number of tasks to return
    
    Returns:
        List of crawl statuses
    """
    # Get the tasks
    tasks = task_manager.list_tasks(task_type="crawl", limit=limit)
    
    # Convert to CrawlStatus objects
    crawl_statuses = []
    for task in tasks:
        result = task.get("result", {})
        crawl_results = result.get("crawl_results", {})
        links_found = crawl_results.get("total_links_found", 0)
        pages_scraped = crawl_results.get("total_pages_scraped", 0)
        
        # Check if clusters are ready
        clusters_ready = (
            result.get("cluster_complete", False) and
            result.get("year_extraction_complete", False)
        )
        
        crawl_statuses.append(CrawlStatus(
            id=task["id"],
            status=task["status"],
            progress=task["progress"],
            current_stage=task["status"],
            links_found=links_found,
            pages_scraped=pages_scraped,
            error=task.get("error"),
            clusters_ready=clusters_ready
        ))
    
    return crawl_statuses

# Routes for clusters
@app.get("/api/clusters", tags=["Clusters"])
async def get_clusters(
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to get clusters for")
):
    """
    Get available clusters for scraping.
    
    Args:
        crawl_task_id: ID of the crawl task to get clusters for
    
    Returns:
        List of clusters
    """
    # If a crawl task ID is provided, get the clusters file from the task result
    url_clusters_file = None
    
    if crawl_task_id:
        task_status = task_manager.get_task_status(crawl_task_id)
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {crawl_task_id} not found")
        
        # Check if the task is completed and has cluster results
        if task_status["status"] != "completed":
            raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} is not completed")
        
        result = task_status.get("result", {})
        if not result.get("cluster_complete", False):
            raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} does not have cluster results")
        
        # Get the clusters file
        url_clusters_file = result.get("output_files", {}).get("url_clusters_file")
    
    # Get available clusters
    clusters = orchestrator.get_available_clusters(url_clusters_file)
    
    return clusters

@app.get("/api/years", tags=["Clusters"])
async def get_years(
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to get years for")
):
    """
    Get available years for downloading.
    
    Args:
        crawl_task_id: ID of the crawl task to get years for
    
    Returns:
        List of years
    """
    # If a crawl task ID is provided, get the years file from the task result
    year_clusters_file = None
    
    if crawl_task_id:
        task_status = task_manager.get_task_status(crawl_task_id)
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {crawl_task_id} not found")
        
        # Check if the task is completed and has year extraction results
        if task_status["status"] != "completed":
            raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} is not completed")
        
        result = task_status.get("result", {})
        if not result.get("year_extraction_complete", False):
            raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} does not have year extraction results")
        
        # Get the years file
        year_clusters_file = result.get("output_files", {}).get("year_clusters_file")
    
    # Get available years
    years = orchestrator.get_available_years(year_clusters_file)
    
    return years

# Routes for scraping and downloading
@app.post("/api/scrape", response_model=ScrapingStatus, tags=["Scraping"])
async def start_scrape(
    request: ScrapingRequest, 
    background_tasks: BackgroundTasks,
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to use for scraping")
):
    """
    Start scraping and downloading with the specified parameters.
    
    Args:
        request: Scraping request parameters
        crawl_task_id: ID of the crawl task to use for scraping
    
    Returns:
        Scraping status with task ID
    """
    # If a crawl task ID is provided, get the files from the task result
    url_clusters_file = None
    year_clusters_file = None
    
    if crawl_task_id:
        task_status = task_manager.get_task_status(crawl_task_id)
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {crawl_task_id} not found")
        
        # Check if the task is completed
        if task_status["status"] != "completed":
            raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} is not completed")
        
        result = task_status.get("result", {})
        output_files = result.get("output_files", {})
        
        # Get the files
        url_clusters_file = output_files.get("url_clusters_file")
        year_clusters_file = output_files.get("year_clusters_file")
    
    # Create a new task
    task_id = task_manager.create_task(
        task_type="scrape",
        params={
            "cluster_ids": request.cluster_ids,
            "years": request.years,
            "url_clusters_file": url_clusters_file,
            "year_clusters_file": year_clusters_file
        }
    )
    
    # Start the scrape in the background
    background_tasks.add_task(
        orchestrator.run_scrape_download,
        task_id=task_id,
        cluster_ids=request.cluster_ids,
        years=request.years,
        url_clusters_file=url_clusters_file,
        year_clusters_file=year_clusters_file
    )
    
    # Return the task status
    task_status = task_manager.get_task_status(task_id)
    
    return ScrapingStatus(
        id=task_id,
        status=task_status["status"],
        progress=task_status["progress"],
        pages_scraped=0,
        files_downloaded=0,
        error=task_status.get("error")
    )

@app.get("/api/scrape/{task_id}", response_model=ScrapingStatus, tags=["Scraping"])
async def get_scrape_status(task_id: str):
    """
    Get the status of a scrape task.
    
    Args:
        task_id: ID of the task
    
    Returns:
        Scraping status
    """
    # Get the task status
    task_status = task_manager.get_task_status(task_id)
    
    if not task_status:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    # Check if the task is a scrape task
    if task_status["type"] != "scrape":
        raise HTTPException(status_code=400, detail=f"Task {task_id} is not a scrape task")
    
    # Extract relevant information
    result = task_status.get("result", {})
    
    # Get scraper-specific information
    scrape_results = result.get("scrape_results", {})
    pages_scraped = scrape_results.get("pages_scraped", 0)
    
    # Get downloader-specific information
    download_results = result.get("download_results", {})
    files_downloaded = download_results.get("files_downloaded", 0)
    
    return ScrapingStatus(
        id=task_id,
        status=task_status["status"],
        progress=task_status["progress"],
        pages_scraped=pages_scraped,
        files_downloaded=files_downloaded,
        error=task_status.get("error")
    )

@app.get("/api/scrape", response_model=List[ScrapingStatus], tags=["Scraping"])
async def list_scrape_tasks(
    limit: int = Query(10, description="Maximum number of tasks to return")
):
    """
    List recent scrape tasks.
    
    Args:
        limit: Maximum number of tasks to return
    
    Returns:
        List of scraping statuses
    """
    # Get the tasks
    tasks = task_manager.list_tasks(task_type="scrape", limit=limit)
    
    # Convert to ScrapingStatus objects
    scrape_statuses = []
    for task in tasks:
        result = task.get("result", {})
        
        # Get scraper-specific information
        scrape_results = result.get("scrape_results", {})
        pages_scraped = scrape_results.get("pages_scraped", 0)
        
        # Get downloader-specific information
        download_results = result.get("download_results", {})
        files_downloaded = download_results.get("files_downloaded", 0)
        
        scrape_statuses.append(ScrapingStatus(
            id=task["id"],
            status=task["status"],
            progress=task["progress"],
            pages_scraped=pages_scraped,
            files_downloaded=files_downloaded,
            error=task.get("error")
        ))
    
    return scrape_statuses

# WebSocket related endpoints
@app.get("/api/websocket/info", tags=["WebSocket"])
async def websocket_info():
    """
    Get information about the WebSocket API.
    
    Returns:
        WebSocket API information
    """
    return {
        "websocket_url": "/ws",
        "events": {
            "subscribe_task": {
                "description": "Subscribe to real-time updates for a task",
                "parameters": {"task_id": "string (required)"}
            },
            "task_update": {
                "description": "Event emitted when task status changes",
                "data_format": {
                    "id": "Task ID",
                    "type": "Task type (crawl, scrape)",
                    "status": "Current status",
                    "progress": "Progress percentage (0-100)",
                    "links_found": "Number of links found (crawl tasks)",
                    "pages_scraped": "Number of pages scraped",
                    "files_downloaded": "Number of files downloaded (scrape tasks)"
                }
            },
            "log": {
                "description": "Event emitted for task log messages",
                "data_format": {
                    "task_id": "Task ID",
                    "timestamp": "ISO timestamp",
                    "level": "Log level (debug, info, warning, error)",
                    "message": "Log message"
                }
            }
        }
    }

@app.get("/api/logs/{task_id}", response_model=LogResponse, tags=["Logs"])
async def get_task_logs(task_id: str):
    """
    Get and clear logs for a specific task.
    
    Args:
        task_id: ID of the task
    
    Returns:
        List of log entries
    """
    # Check if task exists
    task = task_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    # Get logs for the task
    logs = task_manager.get_and_clear_logs(task_id)
    
    return LogResponse(
        logs=logs,
        count=len(logs)
    )

@app.post("/api/crawl/{task_id}/stop", tags=["Crawling"])
async def stop_crawl_task(task_id: str) -> Dict[str, Any]:
    """
    Stop a running crawl task with graceful cleanup.
    
    Args:
        task_id: ID of the task to stop
    
    Returns:
        Dictionary with stop result
    """
    task_status = task_manager.get_task_status(task_id)
    
    if not task_status:
        # Return 404 if task doesn't exist
        return Response(
            status_code=status.HTTP_404_NOT_FOUND,
            content=json.dumps({"detail": f"Task {task_id} not found"}),
            media_type="application/json"
        )
    
    # Check if task is a crawl task
    if task_status.get("type") != "crawl":
        # Return 400 if task is not a crawl task
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=json.dumps({"detail": f"Task {task_id} is not a crawl task"}),
            media_type="application/json"
        )
    
    # Try to stop the crawl task
    stop_result = orchestrator.stop_crawl(task_id)
    
    if stop_result.get("success"):
        # Return 200 if successfully stopped
        return {
            "id": task_id,
            "status": "stopped",
            "message": stop_result.get("message"),
            "cleanup_completed": stop_result.get("cleanup_completed", False)
        }
    else:
        # Return 500 if failed to stop
        return Response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=json.dumps({
                "detail": stop_result.get("message", "Failed to stop task"),
                "id": task_id
            }),
            media_type="application/json"
        )

# Health check route
@app.get("/health", tags=["Health"])
async def health_check():
    """
    Check if the API is running.
    
    Returns:
        Health status
    """
    return {"status": "ok"}

# Background task to listen for Redis messages and forward to WebSocket clients
async def redis_listener():
    """Background task to listen for Redis messages and forward to WebSocket clients."""
    try:
        # Create Redis client for pubsub
        redis = RedisClient(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD
        )
        
        # Create Redis pubsub for task updates
        pubsub_tasks = redis.psubscribe("task:*")  # Subscribe to all task channels
        
        # Create Redis pubsub for logs
        pubsub_logs = redis.psubscribe("logs:*")  # Subscribe to all log channels
        
        # Main pubsub loop
        while True:
            # Process task updates
            task_message = redis.get_message(pubsub_tasks, timeout=0.01)
            if task_message and task_message['type'] == 'pmessage':
                try:
                    channel = task_message['channel']
                    if isinstance(channel, bytes):
                        channel = channel.decode('utf-8')
                    
                    data = json.loads(task_message['data'])
                    
                    # Extract task_id from channel
                    task_id = channel.split(':', 1)[1] if ':' in channel else None
                    
                    if task_id:
                        # Forward to Socket.IO clients in the task room
                        await sio.emit('task_update', data, room=f'task:{task_id}')
                except Exception as e:
                    logger.error(f"Error processing task update: {str(e)}")
            
            # Process log messages
            log_message = redis.get_message(pubsub_logs, timeout=0.01)
            if log_message and log_message['type'] == 'pmessage':
                try:
                    channel = log_message['channel']
                    if isinstance(channel, bytes):
                        channel = channel.decode('utf-8')
                    
                    data = json.loads(log_message['data'])
                    
                    # Extract task_id from channel
                    task_id = channel.split(':', 1)[1] if ':' in channel else None
                    
                    if task_id:
                        # Forward to Socket.IO clients in the log room
                        await sio.emit('log', data, room=f'logs:{task_id}')
                except Exception as e:
                    logger.error(f"Error processing log message: {str(e)}")
            
            # Short sleep to prevent high CPU usage
            await asyncio.sleep(0.01)
    
    except Exception as e:
        logger.error(f"Redis listener error: {str(e)}")
        # Restart after a delay
        await asyncio.sleep(5)
        asyncio.create_task(redis_listener())

# Start Redis listener on application startup
@app.on_event("startup")
async def startup_event():
    """Start background tasks when FastAPI app starts."""
    # Start Redis listener task
    asyncio.create_task(redis_listener())
    logger.info("Redis listener started")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
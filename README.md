# Apollo

Apollo is a backend service for automated web crawling, content scraping, file downloading, and deal scraping. It also handles scheduled execution of these tasks and provides real-time progress updates over WebSocket connections.

---

## What it does

Apollo runs several independent workflows:

**Web Crawling** - Crawls a given website starting from a base URL. Discovers links, respects depth limits and page/link caps, and saves all found links to disk.

**Link Processing** - Takes the raw links found during a crawl and sorts them into categories: file links, bank links, social media links, and miscellaneous links.

**URL Clustering** - Groups bank links by domain and URL path patterns so similar pages are batched together for scraping.

**Year Extraction** - Reads the file links and tries to extract a year from each URL so files can be organized by year.

**Content Scraping** - Visits the URLs in each cluster, extracts the page content, cleans it, converts it to Markdown, and saves it along with a metadata file.

**File Downloading** - Downloads the file links (PDFs, Excel files, etc.) organized by year into a local folder with metadata files.

**Deal Scraping** - Fetches restaurant deals from the Peekaboo API across cities, saves them as Markdown and JSON files, and stores a summary in the database.

**Facebook Scraping** - Pulls posts from a Facebook page via the Graph API, filters them by keywords, categorizes the content, and saves matched posts with metadata.

**Scheduled Runs** - All three scraping types (crawl, deal, Facebook) can be scheduled to run on a specific day and time each week using APScheduler.

**Real-time Updates** - Every running task emits progress and log updates over Socket.IO so clients can track what is happening without polling.

---

## Tech stack

- Python 3.11
- FastAPI for the HTTP API
- Socket.IO for WebSocket connections
- MongoDB with Beanie as the ODM
- APScheduler for scheduled jobs
- Cloudscraper for bypassing bot protection during crawling
- BeautifulSoup for HTML parsing
- Markdownify for converting HTML to Markdown
- ThreadPoolExecutor for parallel processing across all services

---

## Project structure

```
app/
  api/
    routes/          # FastAPI route handlers
  controllers/       # Business logic between routes and services
  models/
    apollo_scrape/   # Pydantic request/response models for crawl and scrape
    fb_scrape/       # Pydantic models for Facebook scraping
    restaurant_deal/ # Pydantic models for deal scraping
    database/        # Beanie document models stored in MongoDB
  services/
    apollo_scrape/   # Apollo crawler, link processor, clusterer, year extractor, scraper, downloader
    fb_scrape/       # Facebook scraping service
    restaurant_deal/ # Deal scraping service
    schedule_service.py  # APScheduler setup and job management
  utils/
    orchestrator.py  # Coordinates full workflows end to end
    task_manager.py  # In-memory task tracking with status and logs
    socket_manager.py  # Socket.IO server and event handling
    realtime_publisher.py  # Pushes task updates to subscribed clients
    config.py        # Environment variable loading
    database.py      # MongoDB connection and Beanie initialization
```

---

## API overview

### Crawl

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/crawl | Start a new crawl |
| GET | /api/crawl/{task_id} | Get crawl status |
| POST | /api/crawl/{task_id}/stop | Stop a running crawl |

### Clusters

| Method | Path | Description |
|--------|------|-------------|
| GET | /api/clusters | Get clusters and years from the latest crawl result |
| GET | /api/get-clusters | List all crawl results with summaries |
| GET | /api/tasks/{task_id}/clusters/{cluster_id} | Get URLs in a specific cluster |
| GET | /api/tasks/{task_id}/years/{year} | Get files for a specific year |

### Scrape and Download

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/scrape | Start scraping clusters and downloading year files |
| GET | /api/scrape/{task_id} | Get scrape task status |

### Deal Scraping

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/deals | Start deal scraping |
| POST | /api/deals/{task_id}/stop | Stop a running deal scrape |
| GET | /api/deals/results | List past deal scrape results |
| GET | /api/deals/result/{task_id} | Get a specific deal result |

### Facebook Scraping

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/facebook | Start Facebook scraping |
| POST | /api/facebook/{task_id}/stop | Stop a running Facebook scrape |
| GET | /api/facebook/results | List past Facebook scrape results |
| GET | /api/facebook/result/{task_id} | Get a specific Facebook result |

### Scheduling

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/schedule | Create or update a crawl schedule |
| GET | /api/schedule | List crawl schedules |
| PUT | /api/schedule/{id} | Update a crawl schedule |
| DELETE | /api/schedule/{id} | Delete a schedule |
| POST | /api/schedule/{id}/pause | Pause a schedule |
| POST | /api/schedule/{id}/resume | Resume a schedule |

The same pattern applies for deal schedules at `/api/deals/schedule` and Facebook schedules at `/api/facebook/schedule`.

### User Preferences

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/user-preference | Save selected clusters and years |
| GET | /api/user-preference | Get saved preferences |

---

## Environment variables

```
MONGODB_URL
DATABASE_NAME
MONGODB_MIN_POOL_SIZE
MONGODB_MAX_POOL_SIZE
MONGODB_MAX_IDLE_TIME
MONGODB_CONNECT_TIMEOUT
MONGODB_SERVER_SELECTION_TIMEOUT

BANK_NAME                    # One of: UBL, FBL, BAFL
CRAWLER_USER_AGENT
CRAWLER_TIMEOUT
CRAWLER_NUM_WORKERS
CRAWLER_DELAY_BETWEEN_REQUESTS
CRAWLER_INACTIVITY_TIMEOUT
CRAWLER_SAVE_INTERVAL
CRAWLER_RESPECT_ROBOTS_TXT
CLUSTER_MIN_SIZE
CLUSTER_PATH_DEPTH
CLUSTER_SIMILARITY_THRESHOLD
EXPIRY_DAYS
MAX_DOWNLOAD_WORKERS
DATA_DIR

ACCESS_TOKEN                 # Facebook Graph API access token
PAGE_ID                      # Facebook page ID

DOCUMENT_BULK_URL            # External API for bulk document ingestion
TAGS_TO_REMOVE               # Comma-separated HTML tags to strip during scraping
CLASSES_TO_REMOVE            # Comma-separated CSS classes to strip during scraping
```

---

## Running locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

---

## Running with Docker

```bash
docker build -t apollo .
docker run -p 8000:8000 --env-file .env apollo
```

---

## WebSocket usage

Connect to the server using Socket.IO. After connecting, subscribe to a task to receive live updates.

```javascript
const socket = io("http://localhost:8000");

socket.emit("subscribe_task", { task_id: "your-task-id" });

socket.on("task_status_update", (data) => {
  console.log(data);
});

socket.on("task_logs_update", (data) => {
  console.log(data.logs);
});

socket.on("task_completed", (data) => {
  console.log("Done", data);
});
```

To receive alerts when a scheduled job runs, emit `subscribe_schedule_alerts` and listen for `schedule_alert` events.

---

## Health check

```
GET /health
```

Returns the status of the database connection, scheduler service, and WebSocket server.

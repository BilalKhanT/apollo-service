import os
import json
import requests
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import logging
import time
import threading
from typing import Dict, List, Any, Optional, Tuple, Callable, Union

class FileDownloader:
    
    def __init__(
        self,
        max_workers: int = 20,  # Increased default to 20 workers
        timeout: int = 30,
        chunk_size: int = 8192,
        progress_update_interval: int = 5,
        retry_count: int = 3,
        batch_size: int = 50  # Increased batch size for better throughput
    ):
        """
        Initialize the FileDownloader with enhanced progress tracking.

        Args:
            max_workers: Maximum number of concurrent downloads
            timeout: Timeout for download requests in seconds
            chunk_size: Size of chunks when streaming large files
            progress_update_interval: How often to update progress (files or percentage)
            retry_count: Number of retries for failed downloads
            batch_size: Number of URLs to process in each batch (helps with memory usage)
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.max_workers = max_workers
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.progress_update_interval = progress_update_interval
        self.retry_count = retry_count
        self.batch_size = batch_size
        
        # Status tracking
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.files_downloaded = 0
        self.files_failed = 0
        self.files_processed = 0
        self.total_files = 0
        self.error = None
        self.current_year = None
        self.current_file = None
        
        # Thread safety with more granular locks
        self.counter_lock = threading.Lock()
        self.year_folder_locks = {}  # Per-year locks to reduce contention
        self.global_folder_lock = threading.Lock() 
        
        # For task manager integration
        self.task_id = None
        self.task_manager = None
        
        # For callback function
        self.progress_callback = None
        
        # Improved session pool management
        self.session_pool = []
        self.session_pool_lock = threading.Lock()
        
        # Results tracking
        self.results = {
            "status": "initialized",
            "total": 0,
            "successful": 0,
            "failed": 0,
            "by_year": {},
            "execution_time_seconds": 0
        }
        self.results_lock = threading.Lock()
        
        self.logger.info(f"FileDownloader initialized with {max_workers} workers, {timeout}s timeout, batch_size={batch_size}")
    
    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("FileDownloader")
        logger.setLevel(logging.INFO)
        
        # Only add handler if not already added
        if not logger.handlers:
            # Create console handler
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def set_task_id(self, task_id: str) -> None:
        """
        Set task ID for integration with a task manager.
        
        Args:
            task_id: Task ID to use for progress reporting
        """
        self.task_id = task_id
        self.logger.info(f"Task ID set to {task_id}")
    
    def set_progress_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        Set a callback function for progress reporting.
        
        Args:
            callback: Function that accepts a dictionary of progress information
        """
        self.progress_callback = callback
        self.logger.info("Progress callback function set")
    
    def publish_progress(self, force: bool = False) -> None:
        """
        Publish current progress to task manager and/or callback function.
        
        Args:
            force: Force publish even if interval conditions not met
        """
        # Only publish if we meet the update interval or force is True
        should_update = (
            force or 
            (self.files_processed % self.progress_update_interval == 0) or
            (self.total_files > 0 and 
             (self.files_processed / self.total_files * 100) % self.progress_update_interval < 
             (1 / self.total_files * 100))
        )
        
        if not should_update:
            return
        
        # Calculate progress (between 65-90% as in the pipeline)
        if self.total_files > 0:
            self.progress = 65.0 + (self.files_processed / self.total_files * 25.0)
        
        # Build progress info dictionary
        progress_info = {
            "status": self.status,
            "progress": self.progress,
            "files_downloaded": self.files_downloaded,
            "files_failed": self.files_failed,
            "files_processed": self.files_processed,
            "total_files": self.total_files,
            "current_year": self.current_year,
            "current_file": self.current_file,
            "execution_time_seconds": time.time() - self.start_time if self.start_time > 0 else 0,
            "error": self.error
        }
        
        # Send to task manager if available
        if self.task_id:
            try:
                # Try to import and use the task manager
                # This is done here to avoid circular imports
                from app.utils.task_manager import task_manager
                self.task_manager = task_manager
                
                # Update task status with progress and partial results
                task_manager.update_task_status(
                    self.task_id,
                    progress=self.progress,
                    result={
                        "download_partial_results": {
                            "files_downloaded": self.files_downloaded,
                            "files_failed": self.files_failed,
                            "files_processed": self.files_processed,
                            "total_files": self.total_files,
                            "current_year": self.current_year
                        }
                    }
                )
                
                # Add log entry for significant progress milestones
                if (self.files_processed % 10 == 0 or force) and self.task_manager:
                    task_manager.publish_log(
                        self.task_id,
                        f"Download progress: {self.files_processed}/{self.total_files} files processed, "
                        f"{self.files_downloaded} successful, {self.files_failed} failed, "
                        f"progress: {self.progress:.1f}%",
                        "info"
                    )
            except (ImportError, AttributeError, Exception) as e:
                self.logger.warning(f"Could not update task manager: {str(e)}")
        
        # Send to callback function if available
        if self.progress_callback:
            try:
                self.progress_callback(progress_info)
            except Exception as e:
                self.logger.warning(f"Error in progress callback: {str(e)}")
        
        # Always log progress for significant milestones or on force
        if self.files_processed % 10 == 0 or force:
            self.logger.info(
                f"Progress: {self.files_processed}/{self.total_files} files processed, "
                f"{self.files_downloaded} successful, {self.files_failed} failed, "
                f"{self.progress:.1f}%"
            )
    
    def create_new_session(self):
        """Create and configure a new session."""
        session = requests.Session()
        # Set common headers
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        # Configure connection pooling for better performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=0,  # We handle retries ourselves
            pool_block=False
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
    
    def get_session(self):
        """Get a session from the pool or create a new one."""
        with self.session_pool_lock:
            if self.session_pool:
                return self.session_pool.pop()
            else:
                return self.create_new_session()
    
    def release_session(self, session):
        """Return a session to the pool."""
        with self.session_pool_lock:
            self.session_pool.append(session)
    
    def ensure_folder_exists(self, folder: str) -> None:
        """Thread-safe way to ensure a folder exists."""
        # Get or create a lock for this specific year folder
        lock = self.year_folder_locks.get(folder)
        if lock is None:
            with self.global_folder_lock:
                # Check again in case another thread created it while we were waiting
                lock = self.year_folder_locks.get(folder)
                if lock is None:
                    lock = threading.Lock()
                    self.year_folder_locks[folder] = lock
        
        # Use the year-specific lock to create the directory
        with lock:
            os.makedirs(folder, exist_ok=True)
    
    def _download_file(self, url: str, folder: str, year: str = None, retry_attempts: int = 0) -> Dict[str, Any]:
        # Thread-safe way to update current file and year
        with self.counter_lock:
            self.current_file = url
            if year is not None:
                self.current_year = year
        
        # Create folder if it doesn't exist (thread-safe)
        self.ensure_folder_exists(folder)
            
        session = self.get_session()
        try:
            # Parse filename from URL
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            
            # Handle empty filenames or query params
            if not filename or '?' in filename:
                # Try to extract a better filename
                path_parts = parsed_url.path.rstrip('/').split('/')
                filename = next((part for part in reversed(path_parts) if part), "unnamed_file")
                
                # Clean up query parameters if they exist
                if '?' in filename:
                    filename = filename.split('?')[0]
                
                # If still empty, use a generic name based on domain
                if not filename:
                    domain = parsed_url.netloc.split('.')
                    domain_name = domain[-2] if len(domain) > 1 else domain[0]
                    filename = f"{domain_name}_file_{int(time.time())}"
            
            # Ensure the filename has an extension based on content-type (if needed)
            if '.' not in filename:
                # Will add extension after getting response headers
                extension_needed = True
            else:
                extension_needed = False
            
            # Create file path
            file_path = os.path.join(folder, filename)
            
            # Check if the file already exists
            if os.path.exists(file_path):
                self.logger.info(f"File already exists: {file_path}")
                # Return success without redownloading
                return {
                    "url": url,
                    "success": True,
                    "file_path": file_path,
                    "size": os.path.getsize(file_path),
                    "message": "File already exists",
                    "status_code": 200,
                    "year": year
                }
            
            # Start download
            self.logger.debug(f"Downloading {url} to {file_path}")
            
            # First make a HEAD request to check status and get content type
            head_response = session.head(url, timeout=self.timeout)
            
            # Check if request was successful
            if head_response.status_code != 200:
                error_msg = f"Failed to retrieve headers with status code {head_response.status_code}"
                
                # If we haven't exceeded retry attempts, try again
                if retry_attempts < self.retry_count and head_response.status_code != 404:
                    self.logger.warning(f"{error_msg}, retrying ({retry_attempts+1}/{self.retry_count})...")
                    time.sleep(0.5)  # Add a small delay before retrying
                    return self._download_file(url, folder, year, retry_attempts + 1)
                else:
                    self.logger.error(f"{error_msg}, max retries exceeded")
                    return {
                        "url": url,
                        "success": False,
                        "file_path": None,
                        "message": error_msg,
                        "status_code": head_response.status_code,
                        "year": year
                    }
            
            # If we need to add an extension based on content-type
            if extension_needed:
                content_type = head_response.headers.get('Content-Type', '')
                
                # Map content types to file extensions
                extension_map = {
                    'application/pdf': '.pdf',
                    'application/msword': '.doc',
                    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
                    'application/vnd.ms-excel': '.xls',
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
                    'application/vnd.ms-powerpoint': '.ppt',
                    'application/vnd.openxmlformats-officedocument.presentationml.presentation': '.pptx',
                    'text/plain': '.txt',
                    'text/csv': '.csv',
                    'application/zip': '.zip',
                    'application/x-rar-compressed': '.rar',
                    'application/gzip': '.gz',
                }
                
                # Extract base content type and default to .bin if unknown
                base_content_type = content_type.split(';')[0].strip()
                extension = extension_map.get(base_content_type, '.bin')
                
                # Update filename and file path
                filename = f"{filename}{extension}"
                file_path = os.path.join(folder, filename)
            
            # Now make the actual GET request to download the file
            response = session.get(url, timeout=self.timeout, stream=True)
            
            # Check if request was successful
            if response.status_code != 200:
                error_msg = f"Failed to download with status code {response.status_code}"
                
                # If we haven't exceeded retry attempts, try again
                if retry_attempts < self.retry_count and response.status_code != 404:
                    self.logger.warning(f"{error_msg}, retrying ({retry_attempts+1}/{self.retry_count})...")
                    time.sleep(0.5)  # Add a small delay before retrying
                    return self._download_file(url, folder, year, retry_attempts + 1)
                else:
                    self.logger.error(f"{error_msg}, max retries exceeded")
                    return {
                        "url": url,
                        "success": False,
                        "file_path": None,
                        "message": error_msg,
                        "status_code": response.status_code,
                        "year": year
                    }
            
            # Check file size to avoid downloading huge files
            content_length = int(response.headers.get('Content-Length', 0))
            if content_length > 100_000_000:  # 100 MB limit
                error_msg = f"File too large: {content_length} bytes"
                self.logger.warning(error_msg)
                return {
                    "url": url,
                    "success": False,
                    "file_path": None,
                    "message": error_msg,
                    "status_code": response.status_code,
                    "year": year
                }
            
            # Save file in chunks to handle large files
            file_size = 0
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        file_size += len(chunk)
            
            # Verify file was saved correctly
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                self.logger.debug(f"Downloaded {url} to {file_path} ({file_size} bytes)")
                return {
                    "url": url,
                    "success": True,
                    "file_path": file_path,
                    "size": file_size,
                    "message": f"Successfully downloaded ({file_size} bytes)",
                    "status_code": response.status_code,
                    "year": year
                }
            else:
                error_msg = "File was not saved correctly or is empty"
                self.logger.error(f"{error_msg} for {url}")
                
                # If we haven't exceeded retry attempts, try again
                if retry_attempts < self.retry_count:
                    self.logger.warning(f"{error_msg}, retrying ({retry_attempts+1}/{self.retry_count})...")
                    time.sleep(0.5)  # Add a small delay before retrying
                    return self._download_file(url, folder, year, retry_attempts + 1)
                else:
                    return {
                        "url": url,
                        "success": False,
                        "file_path": None,
                        "message": error_msg,
                        "status_code": response.status_code,
                        "year": year
                    }
                    
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error for {url}: {str(e)}"
            self.logger.error(error_msg)
            
            # If we haven't exceeded retry attempts, try again
            if retry_attempts < self.retry_count:
                self.logger.warning(f"{error_msg}, retrying ({retry_attempts+1}/{self.retry_count})...")
                time.sleep(0.5)  # Add a small delay before retrying
                return self._download_file(url, folder, year, retry_attempts + 1)
            else:
                return {
                    "url": url,
                    "success": False,
                    "file_path": None,
                    "message": error_msg,
                    "status_code": 0,
                    "year": year
                }
        except Exception as e:
            error_msg = f"Error downloading file: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            return {
                "url": url,
                "success": False,
                "file_path": None,
                "message": error_msg,
                "status_code": 0,
                "year": year
            }
        finally:
            self.release_session(session)
    
    def update_results(self, result: Dict[str, Any]) -> None:
        """Thread-safe way to update results data structure."""
        with self.results_lock:
            year = result.get("year", "unknown")
            
            # Initialize year entry if needed
            if year not in self.results["by_year"]:
                self.results["by_year"][year] = {
                    "total": 0,
                    "successful": 0,
                    "failed": 0,
                    "details": []
                }
            
            # Update counters
            if result["success"]:
                self.results["by_year"][year]["successful"] += 1
                self.results["successful"] += 1
            else:
                self.results["by_year"][year]["failed"] += 1
                self.results["failed"] += 1
            
            self.results["by_year"][year]["total"] += 1
            self.results["by_year"][year]["details"].append(result)
            
            # Also update the counter variables used for progress reporting
            with self.counter_lock:
                if result["success"]:
                    self.files_downloaded += 1
                else:
                    self.files_failed += 1
                
                self.files_processed += 1
                self.publish_progress()
    
    def download_files_by_year(
        self,
        json_file: str,
        years_to_download: Optional[List[str]] = None,
        base_folder: str = "downloads",
        task_id: Optional[str] = None,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Dict[str, Any]:
        """
        Download files from the URLs in the JSON file and organize them by year.
        Optimized to process all years in parallel for maximum throughput.

        Args:
            json_file: Path to the JSON file containing URLs by year
            years_to_download: List of years to download. If None, all years will be downloaded
            base_folder: Base folder where year folders will be created
            task_id: Task ID for integration with task manager
            callback: Function to call with progress updates

        Returns:
            Dictionary with download results
        """
        # Set up task ID and callback
        if task_id:
            self.set_task_id(task_id)
        
        if callback:
            self.set_progress_callback(callback)
        
        # Initialize tracking
        self.start_time = time.time()
        self.status = "initializing"
        self.progress = 65.0  # Start at 65% as in the pipeline
        self.files_downloaded = 0
        self.files_failed = 0
        self.files_processed = 0
        self.total_files = 0
        self.error = None
        
        # Initialize results structure
        self.results = {
            "status": "success",
            "total": 0,
            "successful": 0,
            "failed": 0,
            "by_year": {},
            "execution_time_seconds": 0
        }
        
        # Initialize session pool (2x workers for better availability)
        self.session_pool = [self.create_new_session() for _ in range(self.max_workers * 2)]
        
        self.publish_progress(force=True)
        
        try:
            # Create base folder if it doesn't exist
            os.makedirs(base_folder, exist_ok=True)
            self.logger.info(f"Created base folder: {base_folder}")
            
            # Load JSON file
            with open(json_file, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    error_msg = f"Invalid JSON format in file: {json_file}"
                    self.logger.error(error_msg)
                    self.status = "failed"
                    self.error = error_msg
                    self.publish_progress(force=True)
                    return {
                        "status": "failed",
                        "error": error_msg,
                        "successful": 0,
                        "failed": 0,
                        "total": 0
                    }
            
            # Process only specified years or all years if none specified
            years = years_to_download if years_to_download else list(data.keys())
            years_str = ", ".join(str(year) for year in years)
            self.logger.info(f"Processing the following years: {years_str}")
            
            # Update status
            self.status = "counting_files"
            self.publish_progress(force=True)
            
            # Count total files to download for progress tracking
            self.total_files = 0
            for year in years:
                if year in data:
                    self.total_files += len(data[year])
            
            self.results["total"] = self.total_files
            
            self.logger.info(f"Found {self.total_files} files to download across {len(years)} years")
            
            # Update status
            self.status = "downloading"
            self.publish_progress(force=True)
            
            # Create year folders upfront
            for year in years:
                if year in data and data[year]:  # Only create folders for years with files
                    year_folder = os.path.join(base_folder, str(year))
                    self.ensure_folder_exists(year_folder)
                    self.logger.info(f"Created year folder: {year_folder}")
            
            # Prepare all download tasks across all years
            all_tasks = []
            for year in years:
                if year not in data or not data[year]:
                    continue
                
                year_folder = os.path.join(base_folder, str(year))
                for url in data[year]:
                    all_tasks.append((url, year_folder, year))
            
            # Process all downloads in a single thread pool
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks in batches to control memory usage
                futures_list = []
                
                for i in range(0, len(all_tasks), self.batch_size):
                    batch = all_tasks[i:i+self.batch_size]
                    
                    # Submit batch of downloads
                    futures_dict = {
                        executor.submit(self._download_file, url, folder, year): (url, folder, year)
                        for url, folder, year in batch
                    }
                    
                    # Process results as they complete
                    for future in as_completed(futures_dict):
                        url, folder, year = futures_dict[future]
                        
                        try:
                            # Get result from future
                            result = future.result()
                            
                            # Update result tracking (thread-safe)
                            self.update_results(result)
                            
                        except Exception as e:
                            # Handle exceptions from future execution
                            self.logger.error(f"Error processing result for {url}: {str(e)}")
                            self.logger.error(traceback.format_exc())
                            
                            # Create an error result
                            error_result = {
                                "url": url,
                                "success": False,
                                "file_path": None,
                                "message": f"Error in thread execution: {str(e)}",
                                "status_code": 0,
                                "year": year
                            }
                            
                            # Update counters and tracking (thread-safe)
                            self.update_results(error_result)
            
            # Record execution time
            execution_time = time.time() - self.start_time
            self.results["execution_time_seconds"] = execution_time
            
            # Update status
            self.status = "completed"
            self.progress = 90.0  # End at 90% as in the pipeline
            self.publish_progress(force=True)
            
            self.logger.info(
                f"Download summary: {self.results['successful']} successful, "
                f"{self.results['failed']} failed out of {self.results['total']} total files. "
                f"Completed in {execution_time:.2f} seconds."
            )
            
            return self.results
            
        except Exception as e:
            error_msg = f"Error in download_files_by_year: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            
            # Update status
            self.status = "failed"
            self.error = error_msg
            self.publish_progress(force=True)
            
            return {
                "status": "failed",
                "error": error_msg,
                "successful": self.files_downloaded,
                "failed": self.files_failed,
                "total": self.total_files
            }
        finally:
            # Clean up session pool
            for session in self.session_pool:
                try:
                    session.close()
                except:
                    pass
            self.session_pool = []
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the downloader."""
        return {
            'status': self.status,
            'progress': self.progress,
            'files_downloaded': self.files_downloaded,
            'files_failed': self.files_failed,
            'files_processed': self.files_processed,
            'total_files': self.total_files,
            'current_year': self.current_year,
            'current_file': self.current_file,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0,
            'error': self.error
        }
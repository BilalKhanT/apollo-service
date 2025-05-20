import os
import json
import requests
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import logging
import time
from typing import Dict, List, Any, Optional, Tuple, Callable, Union

class FileDownloader:
    """
    A class for downloading files listed in a JSON file and organizing them into folders by year.
    Features enhanced progress tracking and reporting throughout the download process.
    """
    
    def __init__(
        self,
        max_workers: int = 5,
        timeout: int = 30,
        chunk_size: int = 8192,
        progress_update_interval: int = 5,  # Update progress every 5 files or 5% progress
        retry_count: int = 3  # Number of retries for failed downloads
    ):
        """
        Initialize the FileDownloader with enhanced progress tracking.

        Args:
            max_workers: Maximum number of concurrent downloads
            timeout: Timeout for download requests in seconds
            chunk_size: Size of chunks when streaming large files
            progress_update_interval: How often to update progress (files or percentage)
            retry_count: Number of retries for failed downloads
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.max_workers = max_workers
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.progress_update_interval = progress_update_interval
        self.retry_count = retry_count
        
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
        
        # For task manager integration
        self.task_id = None
        self.task_manager = None
        
        # For callback function
        self.progress_callback = None
        
        self.logger.info(f"FileDownloader initialized with {max_workers} workers, {timeout}s timeout")
    
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
    
    def _download_file(self, url: str, folder: str, retry_attempts: int = 0) -> Dict[str, Any]:
        """
        Download a file from a URL and save it to the specified folder with retry logic.

        Args:
            url: URL of the file to download
            folder: Folder to save the file in
            retry_attempts: Current retry attempt number

        Returns:
            Dictionary with download result information
        """
        self.current_file = url
        
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
                    "status_code": 200
                }
            
            # Start download
            self.logger.debug(f"Downloading {url} to {file_path}")
            
            # Use a session to keep connections alive
            with requests.Session() as session:
                # Set user agent and timeout
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                
                # First make a HEAD request to check status and get content type
                head_response = session.head(url, timeout=self.timeout, headers=headers)
                
                # Check if request was successful
                if head_response.status_code != 200:
                    error_msg = f"Failed to retrieve headers with status code {head_response.status_code}"
                    
                    # If we haven't exceeded retry attempts, try again
                    if retry_attempts < self.retry_count:
                        self.logger.warning(f"{error_msg}, retrying ({retry_attempts+1}/{self.retry_count})...")
                        time.sleep(1)  # Add a small delay before retrying
                        return self._download_file(url, folder, retry_attempts + 1)
                    else:
                        self.logger.error(f"{error_msg}, max retries exceeded")
                        return {
                            "url": url,
                            "success": False,
                            "file_path": None,
                            "message": error_msg,
                            "status_code": head_response.status_code
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
                response = session.get(url, timeout=self.timeout, headers=headers, stream=True)
                
                # Check if request was successful
                if response.status_code != 200:
                    error_msg = f"Failed to download with status code {response.status_code}"
                    
                    # If we haven't exceeded retry attempts, try again
                    if retry_attempts < self.retry_count:
                        self.logger.warning(f"{error_msg}, retrying ({retry_attempts+1}/{self.retry_count})...")
                        time.sleep(1)  # Add a small delay before retrying
                        return self._download_file(url, folder, retry_attempts + 1)
                    else:
                        self.logger.error(f"{error_msg}, max retries exceeded")
                        return {
                            "url": url,
                            "success": False,
                            "file_path": None,
                            "message": error_msg,
                            "status_code": response.status_code
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
                        "status_code": response.status_code
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
                        "status_code": response.status_code
                    }
                else:
                    error_msg = "File was not saved correctly or is empty"
                    self.logger.error(f"{error_msg} for {url}")
                    
                    # If we haven't exceeded retry attempts, try again
                    if retry_attempts < self.retry_count:
                        self.logger.warning(f"{error_msg}, retrying ({retry_attempts+1}/{self.retry_count})...")
                        time.sleep(1)  # Add a small delay before retrying
                        return self._download_file(url, folder, retry_attempts + 1)
                    else:
                        return {
                            "url": url,
                            "success": False,
                            "file_path": None,
                            "message": error_msg,
                            "status_code": response.status_code
                        }
                    
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error for {url}: {str(e)}"
            self.logger.error(error_msg)
            
            # If we haven't exceeded retry attempts, try again
            if retry_attempts < self.retry_count:
                self.logger.warning(f"{error_msg}, retrying ({retry_attempts+1}/{self.retry_count})...")
                time.sleep(1)  # Add a small delay before retrying
                return self._download_file(url, folder, retry_attempts + 1)
            else:
                return {
                    "url": url,
                    "success": False,
                    "file_path": None,
                    "message": error_msg,
                    "status_code": 0
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
                "status_code": 0
            }
    
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
        Provides detailed progress tracking and reporting.

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
            
            self.logger.info(f"Found {self.total_files} files to download across {len(years)} years")
            
            # Update status
            self.status = "downloading"
            self.publish_progress(force=True)
            
            # Consolidated results
            results = {
                "status": "success",
                "total": self.total_files,
                "successful": 0,
                "failed": 0,
                "by_year": {},
                "execution_time_seconds": 0
            }
            
            # Process each year
            for year in years:
                # Skip if year is not in the data
                if year not in data:
                    self.logger.warning(f"Year {year} not found in the JSON file. Skipping.")
                    continue
                
                # Update current year
                self.current_year = year
                self.publish_progress(force=True)
                
                urls = data[year]
                if not urls:
                    self.logger.info(f"No files found for year {year}. Skipping.")
                    continue
                
                # Create year folder
                year_folder = os.path.join(base_folder, str(year))
                os.makedirs(year_folder, exist_ok=True)
                self.logger.info(f"Created year folder: {year_folder}")
                
                # Initialize year results
                year_results = {
                    "total": len(urls),
                    "successful": 0,
                    "failed": 0,
                    "details": []
                }
                
                # Use ThreadPoolExecutor for concurrent downloads
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all download tasks
                    futures_to_url = {
                        executor.submit(self._download_file, url, year_folder): url 
                        for url in urls
                    }
                    
                    # Process results as they complete
                    for future in as_completed(futures_to_url):
                        url = futures_to_url[future]
                        
                        try:
                            # Get result from future
                            result = future.result()
                            
                            # Update result tracking
                            if result["success"]:
                                self.files_downloaded += 1
                                year_results["successful"] += 1
                            else:
                                self.files_failed += 1
                                year_results["failed"] += 1
                            
                            # Add detailed result
                            year_results["details"].append(result)
                            
                        except Exception as e:
                            # Handle exceptions from future execution
                            self.logger.error(f"Error processing result for {url}: {str(e)}")
                            self.logger.error(traceback.format_exc())
                            
                            self.files_failed += 1
                            year_results["failed"] += 1
                            
                            # Add error result
                            year_results["details"].append({
                                "url": url,
                                "success": False,
                                "message": f"Error in thread execution: {str(e)}",
                                "status_code": 0
                            })
                            
                        finally:
                            # Update processed count and progress regardless of success/failure
                            self.files_processed += 1
                            self.publish_progress()
                
                # Add year results to consolidated results
                results["by_year"][year] = year_results
                results["successful"] += year_results["successful"]
                results["failed"] += year_results["failed"]
            
            # Record execution time
            execution_time = time.time() - self.start_time
            results["execution_time_seconds"] = execution_time
            
            # Update status
            self.status = "completed"
            self.progress = 90.0  # End at 90% as in the pipeline
            self.publish_progress(force=True)
            
            self.logger.info(
                f"Download summary: {results['successful']} successful, "
                f"{results['failed']} failed out of {results['total']} total files. "
                f"Completed in {execution_time:.2f} seconds."
            )
            
            return results
            
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
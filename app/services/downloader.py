import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import logging
import time
from typing import Dict, List, Any, Optional, Tuple

class FileDownloader:
    """
    A class for downloading files listed in a JSON file and organizing them into folders by year.
    """
    
    def __init__(
        self,
        max_workers: int = 5,
        timeout: int = 30
    ):
        """
        Initialize the FileDownloader.

        Args:
            max_workers: Maximum number of concurrent downloads
            timeout: Timeout for download requests in seconds
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.max_workers = max_workers
        self.timeout = timeout
        
        # Status tracking
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.files_downloaded = 0
        self.total_files = 0
        self.error = None
    
    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("FileDownloader")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _download_file(self, url: str, folder: str) -> Tuple[str, bool, str]:
        """
        Download a file from a URL and save it to the specified folder.

        Args:
            url: URL of the file to download
            folder: Folder to save the file in

        Returns:
            Tuple of (url, success, message)
        """
        try:
            # Parse filename from URL
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            
            # Handle empty filenames
            if not filename:
                filename = "unnamed_file"
            
            # Create file path
            file_path = os.path.join(folder, filename)
            
            # Download file
            self.logger.debug(f"Downloading {url} to {file_path}")
            response = requests.get(url, timeout=self.timeout, stream=True)
            
            # Check if request was successful
            if response.status_code == 200:
                # Save file
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return url, True, f"Successfully downloaded to {file_path}"
            else:
                return url, False, f"Failed to download with status code {response.status_code}"
        
        except Exception as e:
            return url, False, f"Error downloading file: {str(e)}"
    
    def download_files_by_year(
        self,
        json_file: str,
        years_to_download: Optional[List[str]] = None,
        base_folder: str = "downloads"
    ) -> Dict[str, Any]:
        """
        Download files from the URLs in the JSON file and organize them by year.

        Args:
            json_file: Path to the JSON file containing URLs by year
            years_to_download: List of years to download. If None, all years will be downloaded
            base_folder: Base folder where year folders will be created

        Returns:
            Dictionary with download results
        """
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        self.files_downloaded = 0
        self.error = None
        
        try:
            # Create base folder if it doesn't exist
            if not os.path.exists(base_folder):
                os.makedirs(base_folder)
                self.logger.info(f"Created base folder: {base_folder}")
            
            # Load JSON file
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            results = {
                "total": 0,
                "successful": 0,
                "failed": 0,
                "details": {}
            }
            
            # Process only specified years or all years if none specified
            years = years_to_download if years_to_download else data.keys()
            self.logger.info(f"Processing the following years: {', '.join(years)}")
            
            # Count total files to download for progress tracking
            self.total_files = 0
            for year in years:
                if year in data:
                    self.total_files += len(data[year])
            
            # Process each year
            processed_files = 0
            for year in years:
                # Skip if year is not in the data
                if year not in data:
                    self.logger.warning(f"Year {year} not found in the JSON file. Skipping.")
                    continue
                
                urls = data[year]
                
                # Create year folder
                year_folder = os.path.join(base_folder, year)
                if not os.path.exists(year_folder):
                    os.makedirs(year_folder)
                    self.logger.info(f"Created year folder: {year_folder}")
                
                # Download files for this year
                year_results = self._download_files_for_year(urls, year_folder)
                
                # Update results
                results["total"] += year_results["total"]
                results["successful"] += year_results["successful"]
                results["failed"] += year_results["failed"]
                results["details"][year] = year_results
                
                # Update progress
                processed_files += len(urls)
                self.progress = min(99.0, (processed_files / self.total_files) * 100)
                self.files_downloaded = results["successful"]
            
            self.logger.info(f"Download summary: {results['successful']} successful, {results['failed']} failed out of {results['total']} total files")
            
            # Update status
            self.status = "completed"
            self.progress = 100.0
            
            return results
        
        except Exception as e:
            self.logger.error(f"Error processing JSON file: {str(e)}")
            self.status = "error"
            self.error = str(e)
            raise
    
    def _download_files_for_year(self, urls: List[str], folder: str) -> Dict[str, Any]:
        """
        Download all files for a specific year.

        Args:
            urls: List of URLs to download
            folder: Folder to save the files in

        Returns:
            Dictionary with download results for this year
        """
        results = {
            "total": len(urls),
            "successful": 0,
            "failed": 0,
            "details": []
        }
        
        # Use ThreadPoolExecutor for concurrent downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._download_file, url, folder) for url in urls]
            
            for future in futures:
                url, success, message = future.result()
                
                result = {
                    "url": url,
                    "success": success,
                    "message": message
                }
                
                if success:
                    results["successful"] += 1
                else:
                    results["failed"] += 1
                
                results["details"].append(result)
                
                # Update progress
                self.files_downloaded = self.files_downloaded + (1 if success else 0)
        
        return results
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the downloader."""
        return {
            'status': self.status,
            'progress': self.progress,
            'files_downloaded': self.files_downloaded,
            'total_files': self.total_files,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0,
            'error': self.error
        }
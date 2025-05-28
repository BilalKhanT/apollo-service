import json
import os
import re
import cloudscraper
from bs4 import BeautifulSoup
from markdownify import markdownify as md
import time
import hashlib
from urllib.parse import urlparse
from datetime import datetime, timedelta
import logging
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Tuple, Callable, Union

class ClusterScraper:
    
    def __init__(
        self,
        json_file_path: str,
        output_dir: str = "scraped_content",
        metadata_dir: str = "document_metadata",
        expiry_days: Optional[int] = None,
        progress_update_interval: int = 2,  # Update progress every 2 pages or 2% progress
        max_workers: int = 20  # Number of concurrent workers for scraping
    ):
        """
        Initialize the cluster scraper with enhanced progress tracking.

        Args:
            json_file_path: Path to the JSON file containing clusters
            output_dir: Base directory for saving scraped content
            metadata_dir: Directory for saving metadata files
            expiry_days: Number of days until document expiry
            progress_update_interval: How often to update progress (pages or percentage)
            max_workers: Maximum number of concurrent scraping workers
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.json_file_path = json_file_path
        self.output_dir = output_dir
        self.metadata_dir = metadata_dir
        self.expiry_days = expiry_days
        self.progress_update_interval = progress_update_interval
        self.max_workers = max_workers
        
        # Load clusters data
        self.clusters_data = self.load_clusters_json()
        
        # Status tracking
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.pages_scraped = 0
        self.pages_failed = 0
        self.pages_processed = 0
        self.total_pages = 0
        self.current_cluster_id = ""
        self.current_url = ""
        self.error = None
        
        # Thread safety for counters
        self.counter_lock = threading.Lock()
        
        # For task manager integration
        self.task_id = None
        self.task_manager = None
        
        # For callback function
        self.progress_callback = None
        
        self.logger.info(f"ClusterScraper initialized with output_dir={output_dir}, metadata_dir={metadata_dir}, max_workers={max_workers}")
    
    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("ClusterScraper")
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
            (self.pages_processed % self.progress_update_interval == 0) or
            (self.total_pages > 0 and 
             (self.pages_processed / self.total_pages * 100) % self.progress_update_interval < 
             (1 / self.total_pages * 100))
        )
        
        if not should_update:
            return
        
        # Calculate progress (between 5-60% during scraping phase)
        if self.total_pages > 0:
            self.progress = 5.0 + (self.pages_processed / self.total_pages * 55.0)
        
        # Build progress info dictionary
        progress_info = {
            "status": self.status,
            "progress": self.progress,
            "pages_scraped": self.pages_scraped,
            "pages_failed": self.pages_failed,
            "pages_processed": self.pages_processed,
            "total_pages": self.total_pages,
            "current_cluster": self.current_cluster_id,
            "current_url": self.current_url,
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
                        "scrape_partial_results": {
                            "pages_scraped": self.pages_scraped,
                            "pages_failed": self.pages_failed,
                            "pages_processed": self.pages_processed,
                            "total_pages": self.total_pages,
                            "current_cluster": self.current_cluster_id
                        }
                    }
                )
                
                # Add log entry for significant progress milestones
                if (self.pages_processed % 5 == 0 or force) and self.task_manager:
                    task_manager.publish_log(
                        self.task_id,
                        f"Scraping progress: {self.pages_processed}/{self.total_pages} pages processed, "
                        f"{self.pages_scraped} successful, {self.pages_failed} failed, "
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
        if self.pages_processed % 10 == 0 or force:
            self.logger.info(
                f"Progress: {self.pages_processed}/{self.total_pages} pages processed, "
                f"{self.pages_scraped} successful, {self.pages_failed} failed, "
                f"{self.progress:.1f}%"
            )
    
    def load_clusters_json(self) -> Optional[Dict[str, Any]]:
        """Load and parse the JSON file containing cluster information."""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.logger.info(f"Successfully loaded clusters from {self.json_file_path}")
                return data
        except FileNotFoundError:
            self.logger.error(f"Clusters JSON file not found: {self.json_file_path}")
            return None
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON format in file: {self.json_file_path}")
            return None
        except Exception as e:
            self.logger.error(f"Error loading clusters JSON file: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None
    
    def get_cluster_by_id(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """
        Find a cluster by its ID in the nested JSON structure.

        Args:
            cluster_id: The ID of the cluster to find

        Returns:
            The cluster data if found, None otherwise
        """
        if not self.clusters_data:
            return None
        
        # Check top-level clusters first
        for domain, domain_data in self.clusters_data.get("clusters", {}).items():
            if domain_data.get("id") == cluster_id:
                return domain_data
            
            # Check nested clusters
            for sub_cluster in domain_data.get("clusters", []):
                if sub_cluster.get("id") == cluster_id:
                    return sub_cluster
        
        self.logger.warning(f"Cluster with ID '{cluster_id}' not found.")
        return None
    
    def get_scraper(self):
        """Create a scraper object using cloudscraper to bypass Cloudflare."""
        return cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
    
    def fetch_page(self, url: str, retry_attempts: int = 3) -> Optional[Any]:
        """
        Fetch page content using cloudscraper with retry logic.
        
        Args:
            url: URL to fetch
            retry_attempts: Number of retry attempts for transient errors
            
        Returns:
            Response object or None if failed after retries
        """
        self.current_url = url
        
        for attempt in range(retry_attempts + 1):
            try:
                scraper = self.get_scraper()
                response = scraper.get(url, allow_redirects=True, timeout=30)
                
                # Check if URL was redirected to a 404 page
                final_url = response.url
                if "/404" in final_url or (final_url != url and ("not-found" in final_url or "error" in final_url)):
                    self.logger.warning(f"URL redirected to possible 404 page: {url} -> {final_url}")
                    # Return a modified response with 404 status to signal it should be skipped
                    response.status_code = 404
                
                return response
                
            except Exception as e:
                if attempt < retry_attempts:
                    self.logger.warning(f"Error fetching {url} (attempt {attempt+1}/{retry_attempts+1}): {str(e)}. Retrying...")
                    time.sleep(2)  # Add a delay before retrying
                else:
                    self.logger.error(f"Failed to fetch {url} after {retry_attempts+1} attempts: {str(e)}")
                    return None
        
        return None
    
    def parse_and_convert_to_markdown(self, html: str) -> Tuple[str, str, str]:
        """
        Parse HTML content and convert to markdown, removing navigation, headers, footers, etc.
        
        Args:
            html: HTML content to parse and convert
            
        Returns:
            Tuple of (markdown_content, clean_filename, page_title)
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove headers, footers, navigation, and other non-content elements
            for tag in soup.find_all(['header', 'footer', 'nav', 'aside', 'script', 'style', 'div'],
                class_=['mobile-login-field-small-wrapper','sub-page-links-wrapper','header-main-subpages',
                       'related-links-wrapper','content-wrapper','mobile-header-main', 'mm-header-nav-links',
                       'top-bar','login-field-small-wrapper-subpages','form-small-wrapper','side-nav-inner-page',
                       'footer-wrapper','mobile-copyrights-wrapper','privacy-links-wrapper','bread-crums-wrapper',
                       'dcp-form']):
                tag.decompose()
            
            # Find and remove all images
            for img in soup.find_all('img'):
                img.decompose()
            
            # Remove figure elements which might contain images
            for figure in soup.find_all('figure'):
                figure.decompose()
            
            # Remove picture elements (modern responsive image containers)
            for picture in soup.find_all('picture'):
                picture.decompose()
            
            # Remove SVG elements
            for svg in soup.find_all('svg'):
                svg.decompose()
            
            # Remove sections containing "Apply Now" headings
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                if heading.get_text(strip=True).lower() == "apply now":
                    # Find the parent container
                    parent_to_remove = None
                    current = heading
                    for _ in range(3):
                        if current.parent:
                            current = current.parent
                            if current.name in ['section', 'div', 'form']:
                                parent_to_remove = current
                                break
                    
                    if parent_to_remove:
                        parent_to_remove.decompose()
                    else:
                        # If no suitable parent found, remove the heading and any following form
                        for elem in heading.find_next_siblings():
                            if elem.name == 'form' or ('form' in elem.get('class', '')):
                                elem.decompose()
                        heading.decompose()
            
            # Try to find main content
            content = soup.find_all(['article', 'section', 'main', 'div', 'main id', 'p'], 
                                   class_=['content', 'article-body', 'main-content',  'show', 
                                          'main-heading','tab-content inner-txt-bx','container'])
            
            if not content:
                content = soup.body
            
            if not content:
                self.logger.warning("No usable content found.")
                return "", "", ""
            
            # Get the page title
            title_tag = soup.find('title')
            if title_tag and title_tag.string:
                page_title = title_tag.string.strip()
            else:
                # Try to find an h1 tag
                h1_tag = soup.find('h1')
                if h1_tag:
                    page_title = h1_tag.get_text(strip=True)
                else:
                    page_title = "untitled"
            
            # Clean up the title to be a valid filename
            clean_title = re.sub(r'[^\w\s-]', '', page_title).strip()
            clean_title = re.sub(r'[-\s]+', '-', clean_title)
            
            # If title is still empty or just contains special characters that were removed
            if not clean_title:
                clean_title = "untitled-content"
            
            # Convert to Markdown
            markdown = md(str(content), heading_style="ATX")
            
            # Post-process the markdown to remove any remaining image markdown syntax
            markdown = re.sub(r'!\[.*?\]\(.*?\)', '', markdown)
            
            # Also remove any standalone image URLs that might remain
            markdown = re.sub(r'https?://\S+\.(jpg|jpeg|png|gif|svg|webp)(\?\S+)?', '', markdown, flags=re.IGNORECASE)
            
            return markdown, clean_title, page_title
            
        except Exception as e:
            self.logger.error(f"Error parsing HTML: {str(e)}")
            self.logger.error(traceback.format_exc())
            return "", "", ""
    
    def determine_source_from_url(self, url: str) -> str:
        """
        Determine the source type based on the URL.
        
        Args:
            url: The URL to analyze
            
        Returns:
            Source type ('website', 'facebook', or 'manual')
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        if 'facebook.com' in domain or 'fb.com' in domain:
            return 'facebook'
        else:
            return 'website'
    
    def generate_document_id(self, content: str) -> str:
        """
        Generate a SHA-256 checksum for the document content.
        
        Args:
            content: The content to hash
            
        Returns:
            The SHA-256 hash
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def create_metadata_file(
        self, 
        metadata_dir: str, 
        filename_base: str, 
        document_name: str, 
        url: str, 
        content: str, 
        expiry: Optional[str] = None
    ) -> None:
        """
        Create a metadata file with document information.
        
        Args:
            metadata_dir: Directory to save the metadata file
            filename_base: Base filename (without extension)
            document_name: Name/title of the document
            url: URL of the document
            content: Content of the document for generating checksum
            expiry: Expiry date if set
        """
        os.makedirs(metadata_dir, exist_ok=True)
        
        source = self.determine_source_from_url(url)
        document_id = self.generate_document_id(content)
        
        metadata_content = f"document name: {document_name}\n"
        metadata_content += f"document url: {url}\n"
        metadata_content += f"expiry: {expiry if expiry else 'none'}\n"
        metadata_content += f"source: {source}\n"
        metadata_content += f"document_id: {document_id}\n"
        
        metadata_file_path = os.path.join(metadata_dir, f"{filename_base}.meta")
        with open(metadata_file_path, "w", encoding="utf-8") as f:
            f.write(metadata_content)
        
        self.logger.debug(f"Metadata saved to: {metadata_file_path}")
    
    def url_to_directory_path(self, url: str) -> str:
        """
        Convert a URL to a directory path structure.
        
        Args:
            url: URL to convert
            
        Returns:
            Directory path
        """
        # Parse URL to get path components
        parsed_url = url.split("://", 1)
        if len(parsed_url) < 2:
            return "unknown"
        
        domain_and_path = parsed_url[1].split("/", 1)
        domain = domain_and_path[0]
        
        # Create the base directory structure using domain
        dir_path = os.path.join(self.output_dir, domain)
        
        # Add path components if they exist
        if len(domain_and_path) > 1 and domain_and_path[1]:
            path_parts = domain_and_path[1].split("/")
            # Build full directory path
            for part in path_parts[:-1]:  # Exclude the last part as it might be a file
                if part:
                    dir_path = os.path.join(dir_path, part)
        
        return dir_path
    
    def process_url(self, url: str, cluster_id: str, expiry_date: Optional[str]) -> Dict[str, Any]:
        """
        Process a single URL - thread-safe implementation for parallel processing.
        
        Args:
            url: URL to process
            cluster_id: ID of the cluster the URL belongs to
            expiry_date: Expiry date for the document
            
        Returns:
            Dictionary with processing result
        """
        result = {
            "url": url,
            "success": False,
            "error": None
        }
        
        try:
            self.logger.info(f"Scraping: {url}")
            
            # Fetch the page
            response = self.fetch_page(url)
            
            # If fetch failed completely
            if response is None:
                result["error"] = "Failed to fetch page"
                with self.counter_lock:
                    self.pages_failed += 1
                    self.pages_processed += 1
                    self.publish_progress()
                return result
            
            # Only process pages with 200 status code
            if response.status_code != 200:
                result["error"] = f"Status code: {response.status_code}"
                with self.counter_lock:
                    self.pages_failed += 1
                    self.pages_processed += 1
                    self.publish_progress()
                return result
            
            # Parse and convert to markdown
            html = response.text
            markdown_content, filename_base, document_title = self.parse_and_convert_to_markdown(html)
            
            if markdown_content:
                # Create directory structure based on URL path
                dir_path = self.url_to_directory_path(url)
                os.makedirs(dir_path, exist_ok=True)
                
                # Save the markdown content
                file_path = os.path.join(dir_path, f"{filename_base}.md")
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(markdown_content)
                
                self.logger.info(f"Content saved to: {file_path}")
                
                # Create the metadata file
                self.create_metadata_file(
                    self.metadata_dir,
                    filename_base,
                    document_title,
                    url,
                    markdown_content,
                    expiry_date
                )
                
                # Update counters in a thread-safe way
                with self.counter_lock:
                    self.pages_scraped += 1
                    self.pages_processed += 1
                    self.publish_progress()
                
                result["success"] = True
                return result
            else:
                result["error"] = "No meaningful content extracted"
                with self.counter_lock:
                    self.pages_failed += 1
                    self.pages_processed += 1
                    self.publish_progress()
                return result
                
        except Exception as e:
            self.logger.error(f"Error processing {url}: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            result["error"] = str(e)
            with self.counter_lock:
                self.pages_failed += 1
                self.pages_processed += 1
                self.publish_progress()
            
            return result
    
    def scrape_clusters(
        self, 
        cluster_ids: List[str],
        task_id: Optional[str] = None,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Dict[str, Any]:
        """
        Scrape all URLs in the specified clusters with enhanced progress tracking.
        Now uses parallel processing for improved performance.
        
        Args:
            cluster_ids: List of IDs of the clusters to scrape
            task_id: Task ID for integration with task manager
            callback: Function to call with progress updates
            
        Returns:
            Dictionary with scraping results
        """
        # Import threading module here to avoid any circular dependencies
        import threading
        
        # Set up task ID and callback
        if task_id:
            self.set_task_id(task_id)
        
        if callback:
            self.set_progress_callback(callback)
        
        # Initialize tracking
        self.start_time = time.time()
        self.status = "initializing"
        self.progress = 0.0
        self.pages_scraped = 0
        self.pages_failed = 0
        self.pages_processed = 0
        self.total_pages = 0
        self.error = None
        
        self.publish_progress(force=True)
        
        # Create output directories if they don't exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)
        
        # Calculate expiry date if expiry_days is provided
        expiry_date = None
        if self.expiry_days is not None:
            expiry_date = (datetime.now() + timedelta(days=self.expiry_days)).strftime('%Y-%m-%d')
        
        # Update status and progress
        self.status = "counting_pages"
        self.publish_progress(force=True)
        
        # Count total URLs to scrape for progress tracking
        self.total_pages = 0
        valid_clusters = []
        
        for cluster_id in cluster_ids:
            cluster = self.get_cluster_by_id(cluster_id)
            if cluster:
                urls = cluster.get("urls", [])
                self.total_pages += len(urls)
                valid_clusters.append((cluster_id, cluster, urls))
        
        if self.total_pages == 0:
            self.logger.warning("No pages found to scrape in the specified clusters")
            self.status = "completed"
            self.progress = 5.0
            self.publish_progress(force=True)
            
            return {
                "status": "completed",
                "clusters_scraped": [],
                "pages_scraped": 0,
                "pages_failed": 0,
                "execution_time_seconds": time.time() - self.start_time
            }
        
        self.logger.info(f"Found {self.total_pages} pages to scrape across {len(valid_clusters)} clusters")
        
        # Update status and progress
        self.status = "scraping"
        self.progress = 5.0
        self.publish_progress(force=True)
        
        results = {
            "status": "success",
            "clusters_scraped": [],
            "pages_scraped": 0,
            "pages_failed": 0,
            "errors": []
        }
        
        # Process each cluster
        for cluster_id, cluster, urls in valid_clusters:
            self.current_cluster_id = cluster_id
            self.publish_progress(force=True)
            
            self.logger.info(f"Found {len(urls)} URLs in cluster {cluster_id}. Starting parallel scraping...")
            
            cluster_results = {
                "cluster_id": cluster_id,
                "urls_scraped": 0,
                "urls_failed": 0,
                "errors": []
            }
            
            # Process URLs in parallel with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all URL processing tasks
                futures_to_url = {
                    executor.submit(self.process_url, url, cluster_id, expiry_date): url
                    for url in urls
                }
                
                # Process results as they complete
                for future in as_completed(futures_to_url):
                    url = futures_to_url[future]
                    try:
                        result = future.result()
                        
                        if result["success"]:
                            cluster_results["urls_scraped"] += 1
                        else:
                            cluster_results["urls_failed"] += 1
                            cluster_results["errors"].append({
                                "url": url,
                                "error": result["error"]
                            })
                            
                    except Exception as e:
                        self.logger.error(f"Unexpected error in thread for {url}: {str(e)}")
                        self.logger.error(traceback.format_exc())
                        
                        # Update error tracking
                        cluster_results["urls_failed"] += 1
                        cluster_results["errors"].append({
                            "url": url,
                            "error": f"Thread execution error: {str(e)}"
                        })
                        
                        # Update counters if not already done
                        with self.counter_lock:
                            # We don't know if process_url managed to update these counters
                            # But it's better to potentially double-count than to miss updates
                            self.pages_failed += 1
                            self.pages_processed += 1
                            self.publish_progress()
            
            # Add cluster results
            results["clusters_scraped"].append(cluster_results)
        
        # Update final counts from our thread-safe counters
        results["pages_scraped"] = self.pages_scraped
        results["pages_failed"] = self.pages_failed
        results["total_pages"] = self.total_pages
        results["execution_time_seconds"] = time.time() - self.start_time
        
        # Update status
        self.status = "completed"
        self.progress = 60.0  # End at 60% as in the pipeline
        self.publish_progress(force=True)
        
        self.logger.info(
            f"Scraping completed. Scraped {self.pages_scraped} pages from {len(results['clusters_scraped'])} clusters."
        )
        
        return results
    
    def list_available_clusters(self) -> List[Dict[str, Any]]:
        """
        List all available clusters with their IDs.
        
        Returns:
            List of dictionaries with cluster information
        """
        if not self.clusters_data:
            return []
        
        clusters_info = []
        
        # Go through domains
        for domain, domain_data in self.clusters_data.get("clusters", {}).items():
            # Add domain level clusters
            clusters_info.append({
                "id": domain_data.get("id"),
                "name": domain,
                "type": "domain",
                "url_count": domain_data.get("count", 0)
            })
            
            # Add sub-clusters
            for sub_cluster in domain_data.get("clusters", []):
                clusters_info.append({
                    "id": sub_cluster.get("id"),
                    "name": f"{domain} - {sub_cluster.get('path', 'unknown-path')}",
                    "type": "path",
                    "url_count": sub_cluster.get("url_count", 0)
                })
        
        return clusters_info
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the scraper."""
        return {
            'status': self.status,
            'progress': self.progress,
            'pages_scraped': self.pages_scraped,
            'pages_failed': self.pages_failed,
            'pages_processed': self.pages_processed,
            'total_pages': self.total_pages,
            'current_cluster': self.current_cluster_id,
            'current_url': self.current_url,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0,
            'error': self.error
        }
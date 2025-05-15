import cloudscraper
import json
import re
import time
import os
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor
import queue
import threading
from datetime import datetime
import pytz

class Apollo:
    def __init__(
        self,
        base_url,
        output_file="all_links.json",
        max_links_to_scrape=float('inf'),
        max_pages_to_scrape=float('inf'),
        depth_limit=float('inf'),
        domain_restriction=True,
        url_patterns_to_ignore=None,
        scrape_pdfs_and_xls=True,
        delay_between_requests=2.0,
        respect_robots_txt=True,
        user_agent='AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
        timeout=30,
        num_workers=8,
        save_interval=20,
        inactivity_timeout=100
    ):
        """
        Web crawler that collects all links and properly stops when reaching limits or detecting inactivity.
        Tracks and reports URLs that encountered errors during scraping.
        Follows redirects and handles 404 pages and document links properly.
        """
        # Initialize logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.base_url = base_url
        self.output_file = output_file
        self.max_links_to_scrape = max_links_to_scrape
        self.max_pages_to_scrape = max_pages_to_scrape
        self.depth_limit = depth_limit
        self.domain_restriction = domain_restriction
        self.scrape_pdfs_and_xls = scrape_pdfs_and_xls
        self.delay_between_requests = delay_between_requests
        self.respect_robots_txt = respect_robots_txt
        self.user_agent = user_agent
        self.timeout = timeout
        self.num_workers = num_workers
        self.save_interval = save_interval
        self.inactivity_timeout = inactivity_timeout
        
        # Prepare URL patterns to ignore
        if url_patterns_to_ignore is None:
            url_patterns_to_ignore = [
                r'logout', r'login', r'signin', r'signout',
                r'\.(zip|rar|exe|dmg|jpeg|png|gif|mov|jpg|mp3|m4v|avi|mp4|aspx)$',
                r'\.jpg',  # Match .jpg files
                r'/404$',  # Ignore URLs ending with /404
            ]
        if not scrape_pdfs_and_xls:
            url_patterns_to_ignore.append(r'\.(pdf|xls|xlsx)$')
        self.url_patterns_compiled = [re.compile(pattern, re.IGNORECASE) for pattern in url_patterns_to_ignore]
        
        # Parse base domain for domain restriction
        self.base_domain = urlparse(base_url).netloc
        
        # Initialize data structures with thread safety
        self.visited_urls = set()
        self.all_links = set()
        self.links_queue = queue.Queue()
        self.links_queue.put((base_url, 0))  # (url, depth)
        self.robots_txt_cache = {}
        
        # Document links tracking
        self.document_links = set()
        self.document_links_lock = threading.Lock()
        
        # 404 URLs tracking
        self.not_found_urls = set()
        self.not_found_urls_lock = threading.Lock()
        
        # Error URLs tracking
        self.error_urls = {}
        self.error_urls_lock = threading.Lock()
        
        # Other locks for thread safety
        self.visited_urls_lock = threading.Lock()
        self.all_links_lock = threading.Lock()
        self.robots_cache_lock = threading.Lock()
        self.counter_lock = threading.Lock()
        
        # Activity tracking
        self.last_activity_time = time.time()
        self.last_activity_lock = threading.Lock()
        
        # Counters and flags
        self.total_links_processed = 0
        self.total_pages_scraped = 0
        self.stop_event = threading.Event()
        self.initial_processing_done = threading.Event()
        self.active_worker_count = threading.Semaphore(num_workers)
        self.worker_wait_timeout = 3  # Seconds to wait before checking queue again
        
        # Record start time
        self.start_time = time.time()
        
        # Document file extensions
        self.document_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.rtf', '.csv']
        
        # Workers list
        self.workers = []
        
        # Current status
        self.status = "initialized"
        self.progress = 0.0
        
        # Status callback and update timing
        self.status_callback = None
        self.last_status_update = time.time()
        self.status_update_interval = 2.0  # seconds
    
    def register_status_callback(self, callback):
        """
        Register a callback function to receive status updates.
        
        The callback will be called with the current status dictionary whenever the status is updated.
        
        Args:
            callback: Function that takes a status dictionary as its only argument
        """
        self.status_callback = callback
    
    def _setup_logger(self):
        """Set up logging configuration"""
        logger = logging.getLogger("Apollo")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def update_last_activity(self):
        """Update the last activity timestamp"""
        with self.last_activity_lock:
            self.last_activity_time = time.time()
    
    def should_exclude_url(self, url):
        """Check if a URL should be excluded based on patterns or other rules"""
        # Check if URL is a 404 page
        with self.not_found_urls_lock:
            if url in self.not_found_urls:
                return True
        
        # Check if URL is in patterns to ignore
        if any(pattern.search(url) for pattern in self.url_patterns_compiled):
            return True
        
        # Check domain restriction
        if self.domain_restriction and urlparse(url).netloc != self.base_domain:
            return True
        
        # Check robots.txt if enabled
        if self.respect_robots_txt:
            domain = urlparse(url).netloc
            with self.robots_cache_lock:
                if domain not in self.robots_txt_cache:
                    try:
                        robots_url = f"https://{domain}/robots.txt"
                        scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})
                        scraper.headers.update({'User-Agent': self.user_agent})
                        response = scraper.get(robots_url, timeout=self.timeout)
                        if response.status_code == 200:
                            self.robots_txt_cache[domain] = response.text
                        else:
                            self.robots_txt_cache[domain] = ""
                    except Exception as e:
                        self.logger.warning(f"Error fetching robots.txt for {domain}: {str(e)}")
                        self.robots_txt_cache[domain] = ""
            
            # Simple check for "Disallow:" entries
            robots_content = self.robots_txt_cache.get(domain, "")
            path = urlparse(url).path
            for line in robots_content.split('\n'):
                if line.startswith('Disallow:'):
                    disallow_path = line.split(':', 1)[1].strip()
                    if disallow_path and path.startswith(disallow_path):
                        return True
        
        return False
    
    def check_limits_reached(self):
        """Check if any of the limits have been reached"""
        with self.counter_lock:
            if self.total_pages_scraped >= self.max_pages_to_scrape:
                self.logger.info(f"Maximum pages limit reached: {self.total_pages_scraped}/{self.max_pages_to_scrape}")
                return True
            if self.total_links_processed >= self.max_links_to_scrape:
                self.logger.info(f"Maximum links limit reached: {self.total_links_processed}/{self.max_links_to_scrape}")
                return True
        
        # Check for inactivity timeout
        with self.last_activity_lock:
            if (time.time() - self.last_activity_time) >= self.inactivity_timeout:
                self.logger.info(f"Inactivity timeout reached. No new links added for {self.inactivity_timeout} seconds.")
                return True
        
        return False
    
    def is_document_url(self, url):
        """Check if a URL is a document"""
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        return any(path.endswith(ext) for ext in self.document_extensions)
    
    def process_url(self, worker_id, scraper):
        """Process URLs from the queue"""
        self.logger.info(f"Worker {worker_id} started")
        
        while not self.stop_event.is_set():
            # Periodically send status updates
            current_time = time.time()
            if current_time - self.last_status_update >= self.status_update_interval:
                self.get_status()
                self.last_status_update = current_time
                
            # Acquire semaphore to track active worker count
            self.active_worker_count.acquire()
            
            try:
                # First check if limits are reached before getting new URL
                if self.check_limits_reached():
                    self.stop_event.set()
                    self.logger.info(f"Worker {worker_id} stopping due to limits reached")
                    break
                
                # Get a URL from the queue with timeout
                try:
                    current_url, current_depth = self.links_queue.get(timeout=self.worker_wait_timeout)
                except queue.Empty:
                    # If queue is empty, don't exit immediately
                    # Only consider exiting if initial processing is done
                    if self.initial_processing_done.is_set():
                        # Check if all workers are idle and queue is empty
                        if self.links_queue.empty() and self.active_worker_count._value == self.num_workers:
                            self.logger.info(f"Worker {worker_id} exiting - crawl appears complete")
                            break
                    
                    # Release semaphore and try again
                    self.active_worker_count.release()
                    continue
                
                # Flag to track if we've called task_done() for this URL
                task_completed = False
                
                try:
                    # Check and update visited URLs atomically
                    with self.visited_urls_lock:
                        if current_url in self.visited_urls:
                            # Already visited, mark task done and skip
                            self.links_queue.task_done()
                            task_completed = True
                            continue
                        # Mark as visited while still holding the lock
                        self.visited_urls.add(current_url)
                    
                    # Check exclusion patterns
                    if self.should_exclude_url(current_url):
                        # Mark task done and skip
                        if not task_completed:
                            self.links_queue.task_done()
                            task_completed = True
                        continue
                    
                    self.logger.info(f"Worker {worker_id} processing: {current_url} (Depth: {current_depth})")
                    
                    # Check limits again before making the request
                    if self.check_limits_reached():
                        self.stop_event.set()
                        self.logger.info(f"Worker {worker_id} stopping due to limits reached")
                        if not task_completed:
                            self.links_queue.task_done()
                            task_completed = True
                        break
                    
                    # Make the request with allow_redirects=True to follow redirects
                    response = scraper.get(current_url, timeout=self.timeout, allow_redirects=True)
                    
                    # Check if this URL was redirected
                    final_url = response.url
                    redirect_happened = current_url != final_url
                    
                    # If redirected to a 404 page, mark as a not found URL
                    if final_url.endswith('/404') or '404' in final_url.split('/')[-1]:
                        with self.not_found_urls_lock:
                            self.not_found_urls.add(current_url)  # Add original URL to not_found list
                        self.logger.info(f"Worker {worker_id}: URL {current_url} redirected to 404 page")
                        if not task_completed:
                            self.links_queue.task_done()
                            task_completed = True
                        continue
                    
                    # Check if redirected to a document
                    if redirect_happened and self.is_document_url(final_url):
                        # Only add to all_links but NOT to document_links since it's a redirect
                        with self.all_links_lock:
                            self.all_links.add(final_url)  # Add to all links only
                        self.logger.info(f"Worker {worker_id}: Found document via redirect: {final_url}")
                    
                    # If the URL itself is a direct document (not from redirect), add to document_links and all_links
                    if self.is_document_url(current_url) and not redirect_happened:
                        with self.document_links_lock:
                            self.document_links.add(current_url)
                        with self.all_links_lock:
                            self.all_links.add(current_url)
                    
                    # Add the current URL to all_links if it's not a 404
                    if not final_url.endswith('/404') and '404' not in final_url.split('/')[-1]:
                        with self.all_links_lock:
                            self.all_links.add(current_url)  # Add original URL
                            if redirect_happened:
                                self.all_links.add(final_url)  # Also add final URL if redirected
                    
                    # Update counter and progress
                    with self.counter_lock:
                        self.total_pages_scraped += 1
                        # Calculate progress as percentage of max pages
                        if self.max_pages_to_scrape != float('inf'):
                            self.progress = min(100.0, (self.total_pages_scraped / self.max_pages_to_scrape) * 100)
                        
                        # Check limits right after incrementing
                        if self.total_pages_scraped >= self.max_pages_to_scrape:
                            self.stop_event.set()
                            self.logger.info(f"Max pages limit reached: {self.total_pages_scraped}/{self.max_pages_to_scrape}")
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                            break
                    
                    # Only parse HTML content - skip for documents and other non-HTML responses
                    is_html = 'text/html' in response.headers.get('Content-Type', '')
                    if not is_html:
                        self.logger.info(f"Worker {worker_id}: Non-HTML content at {final_url}, skipping parsing")
                        if not task_completed:
                            self.links_queue.task_done()
                            task_completed = True
                        continue
                    
                    # Parse the HTML with a faster parser
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Find all links efficiently
                    page_links = []
                    for a_tag in soup.find_all('a', href=True):
                        href = a_tag.get('href')
                        if href and not href.startswith(('javascript:', 'mailto:', 'tel:')):
                            # Resolve relative URLs
                            full_url = urljoin(final_url, href)  # Use final_url to resolve
                            # Strip fragments
                            full_url = full_url.split('#')[0]
                            if full_url:
                                page_links.append(full_url)
                    
                    # Remove duplicates
                    page_links = list(set(page_links))
                    
                    # Filter out URLs that should be excluded
                    filtered_links = []
                    for link in page_links:
                        if not self.should_exclude_url(link):
                            filtered_links.append(link)
                    
                    # Update total links count and check limits
                    with self.counter_lock:
                        self.total_links_processed += len(filtered_links)
                        # Save intermediate results periodically
                        if self.total_pages_scraped % self.save_interval == 0:
                            self.save_results(False)  # Non-final save
                        
                        # Check if max links limit is reached
                        if self.total_links_processed >= self.max_links_to_scrape:
                            self.stop_event.set()
                            self.logger.info(f"Max links limit reached: {self.total_links_processed}/{self.max_links_to_scrape}")
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                            break
                    
                    # Add new links to the queue if we haven't reached depth limit
                    if current_depth < self.depth_limit and not self.stop_event.is_set():
                        new_links_added = 0
                        # Use a two-phase approach to avoid race conditions:
                        # 1. First collect URLs that need to be added
                        to_add = []
                        with self.visited_urls_lock:
                            for link in filtered_links:
                                if link not in self.visited_urls:
                                    to_add.append(link)
                        
                        # 2. Then add them to the queue (outside the lock to minimize lock contention)
                        for link in to_add:
                            # Check one more time before adding (another thread could have added it)
                            with self.visited_urls_lock:
                                if link not in self.visited_urls:
                                    self.links_queue.put((link, current_depth + 1))
                                    new_links_added += 1
                        
                        # If we added new links, update the activity timestamp
                        if new_links_added > 0:
                            self.update_last_activity()
                        
                        # Signal that we've processed at least one URL and added links
                        if not self.initial_processing_done.is_set() and new_links_added > 0:
                            self.initial_processing_done.set()
                        
                        self.logger.info(f"Worker {worker_id} added {new_links_added} new links to queue")
                    
                    # Be polite
                    time.sleep(self.delay_between_requests)
                
                except Exception as e:
                    error_msg = str(e)
                    self.logger.error(f"Worker {worker_id} error processing {current_url}: {error_msg}")
                    
                    # Record the error URL and error message
                    with self.error_urls_lock:
                        self.error_urls[current_url] = {
                            "error": error_msg,
                            "timestamp": datetime.now(pytz.timezone("Asia/Karachi")).strftime('%Y-%m-%d %H:%M:%S'),
                            "depth": current_depth
                        }
                
                finally:
                    # Make sure to mark the task as done exactly once
                    if not task_completed:
                        try:
                            self.links_queue.task_done()
                        except ValueError as e:
                            # This catches "task_done() called too many times"
                            self.logger.warning(f"Worker {worker_id}: Queue error - {str(e)}")
            
            except Exception as e:
                self.logger.error(f"Worker {worker_id} unexpected error: {str(e)}")
            
            finally:
                # Release the semaphore
                self.active_worker_count.release()
        
        self.logger.info(f"Worker {worker_id} finished")
    
    def save_results(self, is_final=False):
        """Save crawling results to file"""
        pakistan_time = datetime.now(pytz.timezone("Asia/Karachi"))
        
        # Create a summary
        summary = {
            'base_url': self.base_url,
            'total_urls_discovered': len(self.visited_urls),
            'total_pages_scraped': self.total_pages_scraped,
            'total_links_found': self.total_links_processed,
            'total_unique_links': len(self.all_links),
            'total_direct_document_links': len(self.document_links),
            'total_404_urls': len(self.not_found_urls),
            'total_error_urls': len(self.error_urls),
            'crawl_date': pakistan_time.strftime('%Y-%m-%d %H:%M:%S'),
            'domain_restriction': self.domain_restriction,
            'max_depth': self.depth_limit,
            'is_complete': is_final,
            'execution_time_seconds': time.time() - self.start_time
        }
        
        # Create result with all links
        result_data = {
            'summary': summary,
            'all_links': list(self.all_links),
            'direct_document_links': list(self.document_links),
            '404_urls': list(self.not_found_urls),
            'error_urls': self.error_urls
        }
        
        # Save results
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2)
        
        if is_final:
            self.logger.info(f"Crawl completed. Discovered {len(self.visited_urls)} URLs, scraped {self.total_pages_scraped} pages.")
            self.logger.info(f"Total unique links found: {len(self.all_links)}")
            self.logger.info(f"Total direct document links found: {len(self.document_links)}")
            self.logger.info(f"Total 404 URLs found: {len(self.not_found_urls)}")
            self.logger.info(f"Total URLs with errors: {len(self.error_urls)}")
            self.logger.info(f"Results saved to {self.output_file}")
            self.logger.info(f"Total execution time: {time.time() - self.start_time:.2f} seconds")
    
    def start(self):
        """Start the crawler"""
        # Set status to running
        self.status = "running"
        
        # Log start of crawling
        pakistan_time = datetime.now(pytz.timezone("Asia/Karachi"))
        formatted_time = pakistan_time.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Starting Apollo at {formatted_time} (Pakistan Time)")
        
        # Create a pool of workers
        for i in range(self.num_workers):
            # Each worker gets its own scraper instance
            worker_scraper = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'desktop': True
                }
            )
            worker_scraper.headers.update({'User-Agent': self.user_agent})
            
            # Start the worker thread
            worker = threading.Thread(target=self.process_url, args=(i, worker_scraper))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
        
        try:
            # Monitor the crawling process
            while not self.stop_event.is_set():
                # Check if all workers are idle and queue is empty (crawl naturally completed)
                if self.initial_processing_done.is_set() and self.links_queue.empty() and self.active_worker_count._value == self.num_workers:
                    self.logger.info("Queue empty and no active workers - crawl complete")
                    break
                
                # Check limits periodically from main thread too
                if self.check_limits_reached():
                    self.stop_event.set()
                    self.logger.info("Limits reached - stopping crawl from main thread")
                    break
                
                # Check for periodic status updates
                current_time = time.time()
                if current_time - self.last_status_update >= self.status_update_interval:
                    self.get_status()
                    self.last_status_update = current_time
                
                # Save intermediate results periodically
                self.save_results(False)
                
                # Short sleep to prevent CPU hogging
                time.sleep(1)
        
        except KeyboardInterrupt:
            self.logger.info("Crawl interrupted by user.")
            self.stop_event.set()
            self.status = "interrupted"
        
        # Signal workers to stop
        self.stop_event.set()
        self.logger.info("Stop event set - waiting for workers to finish")
        
        # Wait for all worker threads to finish
        self.logger.info("Waiting for workers to finish...")
        for worker in self.workers:
            worker.join(timeout=5)  # Set a reasonable timeout
        
        # Check for any worker threads that didn't terminate
        active_count = sum(1 for w in self.workers if w.is_alive())
        if active_count > 0:
            self.logger.warning(f"{active_count} worker threads didn't terminate properly")
        
        # Save final results
        self.save_results(True)
        end_time = time.time()
        total_time = end_time - self.start_time
        self.logger.info(f"Total time taken: {total_time:.2f} seconds")
        
        # Update status
        self.status = "completed"
        self.progress = 100.0
        
        # Return the results
        return self.get_results()
    
    def stop(self):
        """Stop the crawler gracefully"""
        if not self.stop_event.is_set():
            self.logger.info("Stopping crawler gracefully...")
            self.stop_event.set()
            self.status = "stopping"
            
            # Wait for workers to finish
            for worker in self.workers:
                worker.join(timeout=5)
            
            # Save final results
            self.save_results(True)
            
            # Update status
            self.status = "stopped"
            
            return True
        return False
    
    def get_status(self):
        """Get the current status of the crawler"""
        status = {
            'status': self.status,
            'progress': self.progress,
            'links_found': self.total_links_processed,
            'pages_scraped': self.total_pages_scraped,
            'execution_time_seconds': time.time() - self.start_time
        }
        
        # Call the callback if registered
        if self.status_callback:
            self.status_callback(status)
        
        return status
    
    def get_results(self):
        """Get the crawling results"""
        return {
            'summary': {
                'base_url': self.base_url,
                'total_urls_discovered': len(self.visited_urls),
                'total_pages_scraped': self.total_pages_scraped,
                'total_links_found': self.total_links_processed,
                'execution_time_seconds': time.time() - self.start_time,
                'total_unique_links': len(self.all_links),
                'total_direct_document_links': len(self.document_links),
                'total_404_urls': len(self.not_found_urls),
                'total_error_urls': len(self.error_urls)
            },
            'all_links': list(self.all_links),
            'direct_document_links': list(self.document_links),
            '404_urls': list(self.not_found_urls),
            'error_urls': self.error_urls
        }
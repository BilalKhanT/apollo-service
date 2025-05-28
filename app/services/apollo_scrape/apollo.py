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
try:
    import dns.resolver
    from cachetools import TTLCache
    DNS_AVAILABLE = True
except ImportError:
    DNS_AVAILABLE = False

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
        num_workers=20,
        save_interval=20,
        inactivity_timeout=100,
        idle_detection_threshold=0.8,  # Percentage of idle workers to consider early termination
        idle_check_interval=5.0,       # Seconds between idle checks
        reduced_inactivity_timeout=10  # Reduced timeout when most workers are idle
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
        self.initial_inactivity_timeout = inactivity_timeout
        self.inactivity_timeout = inactivity_timeout
        
        # Additional parameters for faster completion detection
        self.idle_detection_threshold = idle_detection_threshold
        self.idle_check_interval = idle_check_interval
        self.reduced_inactivity_timeout = reduced_inactivity_timeout
        self.last_idle_check = time.time()
        self.active_workers_count = 0
        self.idle_workers_count = 0
        
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
        
        # Initialize caches based on availability of required packages
        if DNS_AVAILABLE:
            # Add caching for robots.txt and DNS resolution
            self.robots_txt_cache = TTLCache(maxsize=1000, ttl=3600)  # 1 hour TTL
            self.dns_cache = TTLCache(maxsize=1000, ttl=3600)  # 1 hour TTL
            # Initialize DNS resolver with cache
            self.dns_resolver = dns.resolver.Resolver()
            self.dns_resolver.cache = dns.resolver.Cache()
        else:
            # Fall back to normal dictionaries if TTLCache not available
            self.robots_txt_cache = {}
            self.dns_cache = {}
            self.dns_resolver = None
            self.logger.warning("DNS caching disabled - install dnspython and cachetools for better performance")
        
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
        self.dns_cache_lock = threading.Lock()
        
        # Activity tracking
        self.last_activity_time = time.time()
        self.last_activity_lock = threading.Lock()
        self.inactive_time = 0
        
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
        self.status_callbacks = []
        self.last_status_update = time.time()
        self.status_update_interval = 2.0  # seconds
        
        # Queue tracking for progress calculation
        self.initial_queue_size = 1  # Start with the base URL
        self.max_queue_size_reached = 1
        self.url_queue = [(base_url, 0)]
        self.frontier = set()
        
        # Scraper pool for reusing connections
        self.scraper_pool = []
        self.scraper_pool_lock = threading.Lock()
        
        # Create initial scraper pool
        for _ in range(self.num_workers * 2):  # Double the pool size for redundancy
            scraper = self._create_scraper()
            self.scraper_pool.append(scraper)
        
        self.logger.info(f"Apollo initialized with {num_workers} workers and {len(self.scraper_pool)} scrapers in pool")
    
    def _create_scraper(self):
        """Create a new scraper instance with optimal settings"""
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        scraper.headers.update({'User-Agent': self.user_agent})
        
        # Set maximum number of pooled connections
        adapter = scraper.adapters['https://']
        adapter.pool_connections = 100
        adapter.pool_maxsize = 100
        adapter.max_retries = 3
        
        return scraper
    
    def get_scraper(self):
        """Get a scraper from the pool or create a new one"""
        with self.scraper_pool_lock:
            if self.scraper_pool:
                return self.scraper_pool.pop()
            else:
                return self._create_scraper()
    
    def release_scraper(self, scraper):
        """Return a scraper to the pool"""
        with self.scraper_pool_lock:
            self.scraper_pool.append(scraper)
    
    def register_status_callback(self, callback):
        """
        Register a callback function to receive status updates.
        
        The callback will be called with the current status dictionary whenever the status is updated.
        
        Args:
            callback: Function that takes a status dictionary as its only argument
        """
        self.status_callbacks.append(callback)
    
    def _setup_logger(self):
        """Set up logging configuration"""
        logger = logging.getLogger("Apollo")
        logger.setLevel(logging.INFO)
        
        # Create console handler if none exists
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def update_last_activity(self):
        """Update the last activity timestamp"""
        with self.last_activity_lock:
            self.last_activity_time = time.time()
            self.inactive_time = 0
    
    def resolve_domain(self, domain):
        """Resolve domain with caching for faster lookups"""
        if not DNS_AVAILABLE:
            return None
            
        with self.dns_cache_lock:
            if domain in self.dns_cache:
                return self.dns_cache[domain]
            
            try:
                # Resolve the domain
                answers = self.dns_resolver.resolve(domain, 'A')
                ip = answers[0].address
                self.dns_cache[domain] = ip
                return ip
            except Exception as e:
                self.logger.warning(f"Error resolving domain {domain}: {str(e)}")
                return None
    
    def should_exclude_url(self, url):
        """Check if a URL should be excluded based on patterns or other rules"""
        # Check if URL is a 404 page
        with self.not_found_urls_lock:
            if url in self.not_found_urls:
                return True
        
        # Quick check for common patterns before applying regex
        lower_url = url.lower()
        if any(pattern in lower_url for pattern in ['logout', 'login', 'signin', 'signout', '.jpg', '/404']):
            return True
        
        # Check if URL is in patterns to ignore (regex check)
        if any(pattern.search(url) for pattern in self.url_patterns_compiled):
            return True
        
        # Check domain restriction
        parsed_url = urlparse(url)
        if self.domain_restriction and parsed_url.netloc != self.base_domain:
            return True
        
        # Check robots.txt if enabled
        if self.respect_robots_txt:
            domain = parsed_url.netloc
            with self.robots_cache_lock:
                if domain not in self.robots_txt_cache:
                    try:
                        robots_url = f"https://{domain}/robots.txt"
                        scraper = self.get_scraper()
                        try:
                            response = scraper.get(robots_url, timeout=self.timeout)
                            if response.status_code == 200:
                                self.robots_txt_cache[domain] = response.text
                            else:
                                self.robots_txt_cache[domain] = ""
                        finally:
                            self.release_scraper(scraper)
                    except Exception as e:
                        self.logger.warning(f"Error fetching robots.txt for {domain}: {str(e)}")
                        self.robots_txt_cache[domain] = ""
            
            # Simple check for "Disallow:" entries
            robots_content = self.robots_txt_cache.get(domain, "")
            path = parsed_url.path
            for line in robots_content.split('\n'):
                if line.startswith('Disallow:'):
                    disallow_path = line.split(':', 1)[1].strip()
                    if disallow_path and path.startswith(disallow_path):
                        return True
        
        return False
    
    def check_limits_reached(self):
        """Check if any of the limits have been reached or if we should terminate early"""
        with self.counter_lock:
            if self.total_pages_scraped >= self.max_pages_to_scrape:
                self.logger.info(f"Maximum pages limit reached: {self.total_pages_scraped}/{self.max_pages_to_scrape}")
                return True
            if self.total_links_processed >= self.max_links_to_scrape:
                self.logger.info(f"Maximum links limit reached: {self.total_links_processed}/{self.max_links_to_scrape}")
                return True
        
        # Check for inactivity timeout
        with self.last_activity_lock:
            current_time = time.time()
            inactive_duration = current_time - self.last_activity_time
            self.inactive_time = inactive_duration
            
            # Check if most workers are idle to reduce the inactivity timeout
            if self.initial_processing_done.is_set():
                # Count active vs idle workers
                active = self.num_workers - self.active_worker_count._value
                idle = self.active_worker_count._value
                
                # Track for logging
                self.active_workers_count = active
                self.idle_workers_count = idle
                
                # If the queue is empty and most workers are idle, reduce the timeout
                if self.links_queue.empty() and idle/self.num_workers >= self.idle_detection_threshold:
                    # Use reduced timeout after initial processing
                    self.inactivity_timeout = self.reduced_inactivity_timeout
                    
                    # Log this once when we first detect high idle percentage
                    if inactive_duration < 5.0:  # Only log this once
                        self.logger.info(f"Most workers idle ({idle}/{self.num_workers}), reducing inactivity timeout to {self.reduced_inactivity_timeout}s")
                else:
                    # Reset to normal timeout
                    self.inactivity_timeout = self.initial_inactivity_timeout
            
            # Check if we've reached the inactivity timeout
            if inactive_duration >= self.inactivity_timeout:
                self.logger.info(f"Inactivity timeout reached. No new links added for {inactive_duration:.1f} seconds.")
                return True
        
        # Check for early termination condition (periodically)
        current_time = time.time()
        if (current_time - self.last_idle_check >= self.idle_check_interval and 
            self.initial_processing_done.is_set() and 
            self.links_queue.empty()):
            
            self.last_idle_check = current_time
            
            # If ALL workers are idle and queue is empty for more than 5 seconds, stop
            if self.active_worker_count._value == self.num_workers:
                self.logger.info(f"All workers idle and queue empty for {self.idle_check_interval}s - stopping crawler")
                return True
        
        return False
    
    def calculate_progress(self):
        """
        Calculate the current progress as a percentage (0-100).
        Handles cases where limits are unbounded (infinity).
        
        Returns:
            Progress percentage (0-100)
        """
        # Initialize progress
        progress = 0.0
        
        # Calculate progress based on number of pages scraped
        if self.max_pages_to_scrape != float("inf"):
            # If we have a limit, calculate percentage
            if self.max_pages_to_scrape > 0:
                pages_progress = (self.total_pages_scraped / self.max_pages_to_scrape) * 100
                progress = max(progress, min(pages_progress, 100.0))
        else:
            # If unbounded, use a logarithmic scale that approaches but never reaches 100%
            # This gives the user a sense of progress without ever hitting 100% until done
            if self.total_pages_scraped > 0:
                # Log scale that approaches 90% as pages_scraped increases
                # Formula: 90 * (1 - 1/(1 + pages_scraped/100))
                pages_progress = 90.0 * (1.0 - 1.0/(1.0 + self.total_pages_scraped/100.0))
                progress = max(progress, pages_progress)
        
        # Calculate progress based on number of links found
        if self.max_links_to_scrape != float("inf"):
            # If we have a limit, calculate percentage
            if self.max_links_to_scrape > 0:
                links_progress = (self.total_links_processed / self.max_links_to_scrape) * 100
                progress = max(progress, min(links_progress, 100.0))
        else:
            # For unbounded links, use a similar logarithmic scale
            if self.total_links_processed > 0:
                links_progress = 90.0 * (1.0 - 1.0/(1.0 + self.total_links_processed/500.0))
                progress = max(progress, links_progress)
        
        # Calculate progress based on queue size and frontier
        if len(self.url_queue) == 0 and len(self.frontier) == 0:
            # If both queue and frontier are empty, we're done - set to 100%
            progress = 100.0
        elif progress > 85.0 and (len(self.url_queue) < 10 or len(self.frontier) < 10):
            # If we're almost done (queue/frontier almost empty), set progress to 95%
            progress = 95.0
        
        # If we're inactive for a while, gradually increase progress
        if self.inactive_time > 0:
            inactive_progress = min(95.0, 80.0 + (self.inactive_time / self.inactivity_timeout) * 15.0)
            progress = max(progress, inactive_progress)
        
        return progress
    
    def is_document_url(self, url):
        """Check if a URL is a document"""
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        return any(path.endswith(ext) for ext in self.document_extensions)
    
    def extract_links_from_html(self, html_content, base_url):
        """Extract links from HTML content more efficiently"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all links efficiently
        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and not href.startswith(('javascript:', 'mailto:', 'tel:')):
                # Resolve relative URLs
                full_url = urljoin(base_url, href)
                # Strip fragments
                full_url = full_url.split('#')[0]
                if full_url:
                    links.add(full_url)
        
        return links
    
    def filter_links(self, links):
        """Filter links in batch for better efficiency"""
        filtered_links = []
        
        # Pre-check for domain restriction if enabled
        if self.domain_restriction:
            domain_restricted_links = []
            for link in links:
                parsed = urlparse(link)
                if parsed.netloc == self.base_domain:
                    domain_restricted_links.append(link)
            links = domain_restricted_links
        
        # Check URLs against exclusion patterns
        for link in links:
            if not self.should_exclude_url(link):
                filtered_links.append(link)
        
        return filtered_links
    
    def process_url(self, worker_id):
        """Process URLs from the queue"""
        self.logger.info(f"Worker {worker_id} started")
        
        # Get a scraper from the pool
        scraper = self.get_scraper()
        
        try:
            while not self.stop_event.is_set():
                # Periodically send status updates
                current_time = time.time()
                if current_time - self.last_status_update >= self.status_update_interval:
                    self.update_status()
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
                        
                        # Update URL queue tracking for progress calculation
                        with self.counter_lock:
                            if current_url in self.url_queue:
                                self.url_queue.remove((current_url, current_depth))
                            self.frontier.add(current_url)
                            
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
                                
                                # Update frontier tracking
                                with self.counter_lock:
                                    if current_url in self.frontier:
                                        self.frontier.remove(current_url)
                                        
                                continue
                            # Mark as visited while still holding the lock
                            self.visited_urls.add(current_url)
                        
                        # Check exclusion patterns
                        if self.should_exclude_url(current_url):
                            # Mark task done and skip
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                                
                                # Update frontier tracking
                                with self.counter_lock:
                                    if current_url in self.frontier:
                                        self.frontier.remove(current_url)
                                        
                            continue
                        
                        self.logger.info(f"Worker {worker_id} processing: {current_url} (Depth: {current_depth})")
                        
                        # Check limits again before making the request
                        if self.check_limits_reached():
                            self.stop_event.set()
                            self.logger.info(f"Worker {worker_id} stopping due to limits reached")
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                                
                                # Update frontier tracking
                                with self.counter_lock:
                                    if current_url in self.frontier:
                                        self.frontier.remove(current_url)
                                        
                            break
                        
                        # Make the request with allow_redirects=True to follow redirects
                        response = scraper.get(current_url, timeout=self.timeout, allow_redirects=True)
                        
                        # Update frontier tracking
                        with self.counter_lock:
                            if current_url in self.frontier:
                                self.frontier.remove(current_url)
                        
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
                        
                        # Extract links more efficiently
                        page_links = self.extract_links_from_html(response.text, final_url)
                        
                        # Filter links in batch
                        filtered_links = self.filter_links(page_links)
                        
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
                                        
                                        # Update queue tracking for progress calculation
                                        with self.counter_lock:
                                            self.url_queue.append((link, current_depth + 1))
                                            self.max_queue_size_reached = max(self.max_queue_size_reached, 
                                                                             len(self.url_queue) + len(self.frontier))
                            
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
                        
                        # Update frontier tracking
                        with self.counter_lock:
                            if current_url in self.frontier:
                                self.frontier.remove(current_url)
                        
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
        
        finally:
            # Return the scraper to the pool before exiting
            self.release_scraper(scraper)
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
    
    def update_status(self):
        """
        Update and report the current status of the crawler.
        """
        # Calculate progress
        self.progress = self.calculate_progress()
        
        self.status = {
            "status": "running" if not self.stop_event.is_set() else "completed",
            "links_found": self.total_links_processed,
            "pages_scraped": self.total_pages_scraped,
            "progress": self.progress,
            "execution_time_seconds": time.time() - self.start_time,
            "queue_size": len(self.url_queue),
            "frontier_size": len(self.frontier),
            "inactive_time": self.inactive_time
        }
        
        # Call status callbacks if any
        for callback in self.status_callbacks:
            try:
                callback(self.status)
            except Exception as e:
                self.logger.error(f"Error in status callback: {str(e)}")
                
        return self.status
    
    def start(self):
        """Start the crawler"""
        # Set status to running
        self.status = "running"
        
        # Log start of crawling
        pakistan_time = datetime.now(pytz.timezone("Asia/Karachi"))
        formatted_time = pakistan_time.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Starting Apollo at {formatted_time} (Pakistan Time)")
        
        # Create worker threads
        for i in range(self.num_workers):
            # Start the worker thread with improved worker function
            worker = threading.Thread(target=self.process_url, args=(i,))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
        
        try:
            # Monitor the crawling process
            while not self.stop_event.is_set():
                # Check if all workers are idle and queue is empty (crawl naturally completed)
                # This check is now more aggressive with the updated idle detection
                if (self.initial_processing_done.is_set() and 
                    self.links_queue.empty() and 
                    self.active_worker_count._value == self.num_workers):
                    
                    # We now log more detailed information for debugging
                    self.logger.info(f"Queue empty and all workers idle ({self.active_worker_count._value}/{self.num_workers}) - crawl complete")
                    break
                
                # Update inactive time
                with self.last_activity_lock:
                    current_time = time.time()
                    self.inactive_time = current_time - self.last_activity_time
                
                # Check limits periodically from main thread too
                if self.check_limits_reached():
                    self.stop_event.set()
                    self.logger.info("Limits reached - stopping crawl from main thread")
                    break
                
                # Check for periodic status updates
                current_time = time.time()
                if current_time - self.last_status_update >= self.status_update_interval:
                    self.update_status()
                    self.last_status_update = current_time
                
                # Add this right after checking limits, to log worker states periodically
                if time.time() - self.last_idle_check >= self.idle_check_interval:
                    self.last_idle_check = time.time()
                    active = self.num_workers - self.active_worker_count._value
                    idle = self.active_worker_count._value
                    self.logger.debug(f"Worker status: {active} active, {idle} idle, queue size: {self.links_queue.qsize()}")
                
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
        
        # Final status update
        self.update_status()
        
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
            self.progress = 100.0
            
            # Final status update
            self.update_status()
            
            return True
        return False
    
    def get_status(self):
        """Get the current status of the crawler"""
        # Calculate progress
        self.progress = self.calculate_progress()
        
        status = {
            'status': "running" if not self.stop_event.is_set() else "completed",
            'progress': self.progress,
            'links_found': self.total_links_processed,
            'pages_scraped': self.total_pages_scraped,
            'execution_time_seconds': time.time() - self.start_time,
            'queue_size': len(self.url_queue),
            'frontier_size': len(self.frontier),
            'inactive_time': self.inactive_time
        }
        
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
    
    def cleanup(self):
        """Perform cleanup operations"""
        # Clear data structures
        self.visited_urls.clear()
        self.all_links.clear()
        self.document_links.clear()
        self.not_found_urls.clear()
        self.error_urls.clear()
        
        # Clear caches based on their type
        if DNS_AVAILABLE:
            self.robots_txt_cache.clear()
            self.dns_cache.clear()
        else:
            self.robots_txt_cache = {}
            self.dns_cache = {}
        
        # Clear queue and frontiers
        while not self.links_queue.empty():
            try:
                self.links_queue.get_nowait()
                self.links_queue.task_done()
            except queue.Empty:
                break
                
        self.url_queue.clear()
        self.frontier.clear()
        
        # Reset counters and flags
        self.total_links_processed = 0
        self.total_pages_scraped = 0
        
        # Close all scrapers in the pool
        with self.scraper_pool_lock:
            self.scraper_pool.clear()
        
        # Log completion of cleanup
        self.logger.info("Cleanup completed")
        
        return True
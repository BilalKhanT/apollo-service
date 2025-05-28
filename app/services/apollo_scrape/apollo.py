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
        idle_detection_threshold=0.8,
        idle_check_interval=5.0,
        reduced_inactivity_timeout=10
    ):
        self.logger = self._setup_logger()
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
        self.idle_detection_threshold = idle_detection_threshold
        self.idle_check_interval = idle_check_interval
        self.reduced_inactivity_timeout = reduced_inactivity_timeout
        self.last_idle_check = time.time()
        self.active_workers_count = 0
        self.idle_workers_count = 0

        if url_patterns_to_ignore is None:
            url_patterns_to_ignore = [
                r'logout', r'login', r'signin', r'signout',
                r'\.(zip|rar|exe|dmg|jpeg|png|gif|mov|jpg|mp3|m4v|avi|mp4|aspx)$',
                r'\.jpg',
                r'/404$',
            ]
        if not scrape_pdfs_and_xls:
            url_patterns_to_ignore.append(r'\.(pdf|xls|xlsx)$')
        self.url_patterns_compiled = [re.compile(pattern, re.IGNORECASE) for pattern in url_patterns_to_ignore]

        self.base_domain = urlparse(base_url).netloc

        self.visited_urls = set()
        self.all_links = set()
        self.links_queue = queue.Queue()
        self.links_queue.put((base_url, 0))  

        if DNS_AVAILABLE:
            self.robots_txt_cache = TTLCache(maxsize=1000, ttl=3600)
            self.dns_cache = TTLCache(maxsize=1000, ttl=3600)
            self.dns_resolver = dns.resolver.Resolver()
            self.dns_resolver.cache = dns.resolver.Cache()
        else:
            self.robots_txt_cache = {}
            self.dns_cache = {}
            self.dns_resolver = None
            self.logger.warning("DNS caching disabled - install dnspython and cachetools for better performance")

        self.document_links = set()
        self.document_links_lock = threading.Lock()

        self.not_found_urls = set()
        self.not_found_urls_lock = threading.Lock()

        self.error_urls = {}
        self.error_urls_lock = threading.Lock()

        self.visited_urls_lock = threading.Lock()
        self.all_links_lock = threading.Lock()
        self.robots_cache_lock = threading.Lock()
        self.counter_lock = threading.Lock()
        self.dns_cache_lock = threading.Lock()

        self.last_activity_time = time.time()
        self.last_activity_lock = threading.Lock()
        self.inactive_time = 0

        self.total_links_processed = 0
        self.total_pages_scraped = 0

        self.stop_event = threading.Event()
        self.stop_requested = False  
        
        self.initial_processing_done = threading.Event()
        self.active_worker_count = threading.Semaphore(num_workers)
        self.worker_wait_timeout = 3

        self.start_time = time.time()

        self.document_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.rtf', '.csv']

        self.workers = []

        self.status = "initialized"
        self.progress = 0.0

        self.status_callbacks = []
        self.last_status_update = time.time()
        self.status_update_interval = 2.0

        self.initial_queue_size = 1
        self.max_queue_size_reached = 1
        self.url_queue = [(base_url, 0)]
        self.frontier = set()

        self.scraper_pool = []
        self.scraper_pool_lock = threading.Lock()

        for _ in range(self.num_workers * 2):
            scraper = self._create_scraper()
            self.scraper_pool.append(scraper)
        
        self.logger.info(f"Apollo initialized with {num_workers} workers and {len(self.scraper_pool)} scrapers in pool")
    
    def _create_scraper(self):
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        scraper.headers.update({'User-Agent': self.user_agent})
        
        adapter = scraper.adapters['https://']
        adapter.pool_connections = 100
        adapter.pool_maxsize = 100
        adapter.max_retries = 3
        
        return scraper
    
    def get_scraper(self):
        with self.scraper_pool_lock:
            if self.scraper_pool:
                return self.scraper_pool.pop()
            else:
                return self._create_scraper()
    
    def release_scraper(self, scraper):
        with self.scraper_pool_lock:
            self.scraper_pool.append(scraper)
    
    def register_status_callback(self, callback):
        self.status_callbacks.append(callback)
    
    def _setup_logger(self):
        logger = logging.getLogger("Apollo")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def update_last_activity(self):
        if self.stop_requested:
            return
            
        with self.last_activity_lock:
            self.last_activity_time = time.time()
            self.inactive_time = 0
    
    def resolve_domain(self, domain):
        if not DNS_AVAILABLE:
            return None
            
        with self.dns_cache_lock:
            if domain in self.dns_cache:
                return self.dns_cache[domain]
            
            try:
                answers = self.dns_resolver.resolve(domain, 'A')
                ip = answers[0].address
                self.dns_cache[domain] = ip
                return ip
            except Exception as e:
                self.logger.warning(f"Error resolving domain {domain}: {str(e)}")
                return None
    
    def should_exclude_url(self, url):
        if self.stop_event.is_set():
            return True

        with self.not_found_urls_lock:
            if url in self.not_found_urls:
                return True

        lower_url = url.lower()
        if any(pattern in lower_url for pattern in ['logout', 'login', 'signin', 'signout', '.jpg', '/404']):
            return True

        if any(pattern.search(url) for pattern in self.url_patterns_compiled):
            return True

        parsed_url = urlparse(url)
        if self.domain_restriction and parsed_url.netloc != self.base_domain:
            return True

        if self.respect_robots_txt:
            domain = parsed_url.netloc
            with self.robots_cache_lock:
                if domain not in self.robots_txt_cache:
                    try:
                        robots_url = f"https://{domain}/robots.txt"
                        scraper = self.get_scraper()
                        try:
                            response = scraper.get(robots_url, timeout=min(self.timeout, 10))
                            if response.status_code == 200:
                                self.robots_txt_cache[domain] = response.text
                            else:
                                self.robots_txt_cache[domain] = ""
                        finally:
                            self.release_scraper(scraper)
                    except Exception as e:
                        self.logger.warning(f"Error fetching robots.txt for {domain}: {str(e)}")
                        self.robots_txt_cache[domain] = ""

            robots_content = self.robots_txt_cache.get(domain, "")
            path = parsed_url.path
            for line in robots_content.split('\n'):
                if line.startswith('Disallow:'):
                    disallow_path = line.split(':', 1)[1].strip()
                    if disallow_path and path.startswith(disallow_path):
                        return True
        
        return False
    
    def check_limits_reached(self):
        if self.stop_event.is_set() or self.stop_requested:
            self.logger.info("Stop event detected - crawler should terminate")
            return True
            
        with self.counter_lock:
            if self.total_pages_scraped >= self.max_pages_to_scrape:
                self.logger.info(f"Maximum pages limit reached: {self.total_pages_scraped}/{self.max_pages_to_scrape}")
                return True
            if self.total_links_processed >= self.max_links_to_scrape:
                self.logger.info(f"Maximum links limit reached: {self.total_links_processed}/{self.max_links_to_scrape}")
                return True

        if not self.stop_requested:
            with self.last_activity_lock:
                current_time = time.time()
                inactive_duration = current_time - self.last_activity_time
                self.inactive_time = inactive_duration

                if self.initial_processing_done.is_set():
                    active = self.num_workers - self.active_worker_count._value
                    idle = self.active_worker_count._value
                    
                    self.active_workers_count = active
                    self.idle_workers_count = idle
                    
                    if self.links_queue.empty() and idle/self.num_workers >= self.idle_detection_threshold:
                        self.inactivity_timeout = self.reduced_inactivity_timeout
                        
                        if inactive_duration < 5.0:
                            self.logger.info(f"Most workers idle ({idle}/{self.num_workers}), reducing inactivity timeout to {self.reduced_inactivity_timeout}s")
                    else:
                        self.inactivity_timeout = self.initial_inactivity_timeout
                
                if inactive_duration >= self.inactivity_timeout:
                    self.logger.info(f"Inactivity timeout reached. No new links added for {inactive_duration:.1f} seconds.")
                    return True

        current_time = time.time()
        if (current_time - self.last_idle_check >= self.idle_check_interval and 
            self.initial_processing_done.is_set() and 
            self.links_queue.empty()):
            
            self.last_idle_check = current_time
            
            if self.active_worker_count._value == self.num_workers:
                self.logger.info(f"All workers idle and queue empty for {self.idle_check_interval}s - stopping crawler")
                return True
        
        return False
    
    def calculate_progress(self):
        if self.stop_requested or self.stop_event.is_set():
            return min(95.0, self.progress)  
            
        progress = 0.0

        if self.max_pages_to_scrape != float("inf"):
            if self.max_pages_to_scrape > 0:
                pages_progress = (self.total_pages_scraped / self.max_pages_to_scrape) * 100
                progress = max(progress, min(pages_progress, 100.0))
        else:
            if self.total_pages_scraped > 0:
                pages_progress = 90.0 * (1.0 - 1.0/(1.0 + self.total_pages_scraped/100.0))
                progress = max(progress, pages_progress)

        if self.max_links_to_scrape != float("inf"):
            if self.max_links_to_scrape > 0:
                links_progress = (self.total_links_processed / self.max_links_to_scrape) * 100
                progress = max(progress, min(links_progress, 100.0))
        else:
            if self.total_links_processed > 0:
                links_progress = 90.0 * (1.0 - 1.0/(1.0 + self.total_links_processed/500.0))
                progress = max(progress, links_progress)

        if len(self.url_queue) == 0 and len(self.frontier) == 0:
            progress = 100.0
        elif progress > 85.0 and (len(self.url_queue) < 10 or len(self.frontier) < 10):
            progress = 95.0

        if self.inactive_time > 0:
            inactive_progress = min(95.0, 80.0 + (self.inactive_time / self.inactivity_timeout) * 15.0)
            progress = max(progress, inactive_progress)
        
        return progress
    
    def is_document_url(self, url):
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        return any(path.endswith(ext) for ext in self.document_extensions)
    
    def extract_links_from_html(self, html_content, base_url):
        soup = BeautifulSoup(html_content, 'html.parser')
        
        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and not href.startswith(('javascript:', 'mailto:', 'tel:')):
                full_url = urljoin(base_url, href)
                full_url = full_url.split('#')[0]
                if full_url:
                    links.add(full_url)
        
        return links
    
    def filter_links(self, links):
        if self.stop_event.is_set():
            return []
            
        filtered_links = []
        
        if self.domain_restriction:
            domain_restricted_links = []
            for link in links:
                parsed = urlparse(link)
                if parsed.netloc == self.base_domain:
                    domain_restricted_links.append(link)
            links = domain_restricted_links
        
        for link in links:
            if not self.should_exclude_url(link):
                filtered_links.append(link)
        
        return filtered_links
    
    def process_url(self, worker_id):
        self.logger.info(f"Worker {worker_id} started")
        
        scraper = self.get_scraper()
        
        try:
            while not self.stop_event.is_set() and not self.stop_requested:
                if self.stop_event.is_set() or self.stop_requested:
                    self.logger.info(f"Worker {worker_id} stopping due to stop event")
                    break

                current_time = time.time()
                if current_time - self.last_status_update >= self.status_update_interval:
                    self.update_status()
                    self.last_status_update = current_time

                self.active_worker_count.acquire()
                
                try:
                    if self.check_limits_reached():
                        self.stop_event.set()
                        self.logger.info(f"Worker {worker_id} stopping due to limits reached")
                        break

                    try:
                        current_url, current_depth = self.links_queue.get(timeout=self.worker_wait_timeout)

                        if self.stop_event.is_set() or self.stop_requested:
                            self.links_queue.task_done()
                            self.logger.info(f"Worker {worker_id} stopping after getting URL")
                            break

                        with self.counter_lock:
                            if current_url in self.url_queue:
                                self.url_queue.remove((current_url, current_depth))
                            self.frontier.add(current_url)
                            
                    except queue.Empty:
                        if self.initial_processing_done.is_set():
                            if self.links_queue.empty() and self.active_worker_count._value == self.num_workers:
                                self.logger.info(f"Worker {worker_id} exiting - crawl appears complete")
                                break
                        
                        self.active_worker_count.release()
                        continue
                    
                    task_completed = False
                    
                    try:
                        with self.visited_urls_lock:
                            if current_url in self.visited_urls:
                                self.links_queue.task_done()
                                task_completed = True
                                
                                with self.counter_lock:
                                    if current_url in self.frontier:
                                        self.frontier.remove(current_url)
                                        
                                continue
                            self.visited_urls.add(current_url)

                        if self.stop_event.is_set() or self.stop_requested:
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                                
                                with self.counter_lock:
                                    if current_url in self.frontier:
                                        self.frontier.remove(current_url)
                                        
                            break

                        if self.should_exclude_url(current_url):
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                                
                                with self.counter_lock:
                                    if current_url in self.frontier:
                                        self.frontier.remove(current_url)
                                        
                            continue
                        
                        self.logger.info(f"Worker {worker_id} processing: {current_url} (Depth: {current_depth})")

                        if self.check_limits_reached():
                            self.stop_event.set()
                            self.logger.info(f"Worker {worker_id} stopping due to limits reached")
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                                
                                with self.counter_lock:
                                    if current_url in self.frontier:
                                        self.frontier.remove(current_url)
                                        
                            break

                        request_timeout = 5 if self.stop_requested else self.timeout

                        response = scraper.get(current_url, timeout=request_timeout, allow_redirects=True)

                        if self.stop_event.is_set() or self.stop_requested:
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                            
                            with self.counter_lock:
                                if current_url in self.frontier:
                                    self.frontier.remove(current_url)
                            break

                        with self.counter_lock:
                            if current_url in self.frontier:
                                self.frontier.remove(current_url)

                        final_url = response.url
                        redirect_happened = current_url != final_url

                        if final_url.endswith('/404') or '404' in final_url.split('/')[-1]:
                            with self.not_found_urls_lock:
                                self.not_found_urls.add(current_url)
                            self.logger.info(f"Worker {worker_id}: URL {current_url} redirected to 404 page")
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                            continue

                        if redirect_happened and self.is_document_url(final_url):
                            with self.all_links_lock:
                                self.all_links.add(final_url)
                            self.logger.info(f"Worker {worker_id}: Found document via redirect: {final_url}")
                        
                        if self.is_document_url(current_url) and not redirect_happened:
                            with self.document_links_lock:
                                self.document_links.add(current_url)
                            with self.all_links_lock:
                                self.all_links.add(current_url)

                        if not final_url.endswith('/404') and '404' not in final_url.split('/')[-1]:
                            with self.all_links_lock:
                                self.all_links.add(current_url)
                                if redirect_happened:
                                    self.all_links.add(final_url)

                        with self.counter_lock:
                            self.total_pages_scraped += 1
                            
                            if self.total_pages_scraped >= self.max_pages_to_scrape:
                                self.stop_event.set()
                                self.logger.info(f"Max pages limit reached: {self.total_pages_scraped}/{self.max_pages_to_scrape}")
                                if not task_completed:
                                    self.links_queue.task_done()
                                    task_completed = True
                                break

                        is_html = 'text/html' in response.headers.get('Content-Type', '')
                        if not is_html:
                            self.logger.info(f"Worker {worker_id}: Non-HTML content at {final_url}, skipping parsing")
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                            continue

                        if self.stop_event.is_set() or self.stop_requested:
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                            break

                        page_links = self.extract_links_from_html(response.text, final_url)

                        filtered_links = self.filter_links(page_links)

                        if self.stop_event.is_set() or self.stop_requested:
                            if not task_completed:
                                self.links_queue.task_done()
                                task_completed = True
                            break

                        with self.counter_lock:
                            self.total_links_processed += len(filtered_links)
                            if self.total_pages_scraped % self.save_interval == 0:
                                self.save_results(False)
                            
                            if self.total_links_processed >= self.max_links_to_scrape:
                                self.stop_event.set()
                                self.logger.info(f"Max links limit reached: {self.total_links_processed}/{self.max_links_to_scrape}")
                                if not task_completed:
                                    self.links_queue.task_done()
                                    task_completed = True
                                break

                        if (current_depth < self.depth_limit and 
                            not self.stop_event.is_set() and 
                            not self.stop_requested):
                            
                            new_links_added = 0
                            to_add = []
                            with self.visited_urls_lock:
                                for link in filtered_links:
                                    if link not in self.visited_urls:
                                        to_add.append(link)
                            
                            for link in to_add:
                                if self.stop_event.is_set() or self.stop_requested:
                                    break
                                    
                                with self.visited_urls_lock:
                                    if link not in self.visited_urls:
                                        self.links_queue.put((link, current_depth + 1))
                                        new_links_added += 1
                                        
                                        with self.counter_lock:
                                            self.url_queue.append((link, current_depth + 1))
                                            self.max_queue_size_reached = max(self.max_queue_size_reached, 
                                                                             len(self.url_queue) + len(self.frontier))
                            
                            if new_links_added > 0 and not self.stop_requested:
                                self.update_last_activity()

                            if not self.initial_processing_done.is_set() and new_links_added > 0:
                                self.initial_processing_done.set()
                            
                            self.logger.info(f"Worker {worker_id} added {new_links_added} new links to queue")

                        if not self.stop_requested:
                            time.sleep(self.delay_between_requests)
                    
                    except Exception as e:
                        error_msg = str(e)
                        self.logger.error(f"Worker {worker_id} error processing {current_url}: {error_msg}")
                        
                        with self.counter_lock:
                            if current_url in self.frontier:
                                self.frontier.remove(current_url)
                        
                        with self.error_urls_lock:
                            self.error_urls[current_url] = {
                                "error": error_msg,
                                "timestamp": datetime.now(pytz.timezone("Asia/Karachi")).strftime('%Y-%m-%d %H:%M:%S'),
                                "depth": current_depth
                            }
                    
                    finally:
                        if not task_completed:
                            try:
                                self.links_queue.task_done()
                            except ValueError as e:
                                self.logger.warning(f"Worker {worker_id}: Queue error - {str(e)}")
                
                except Exception as e:
                    self.logger.error(f"Worker {worker_id} unexpected error: {str(e)}")
                
                finally:
                    self.active_worker_count.release()
        
        finally:
            self.release_scraper(scraper)
            self.logger.info(f"Worker {worker_id} finished")
    
    def save_results(self, is_final=False):
        pakistan_time = datetime.now(pytz.timezone("Asia/Karachi"))

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
            'execution_time_seconds': time.time() - self.start_time,
            'was_stopped': self.stop_requested or self.stop_event.is_set()  # FIXED: Track if stopped
        }

        result_data = {
            'summary': summary,
            'all_links': list(self.all_links),
            'direct_document_links': list(self.document_links),
            '404_urls': list(self.not_found_urls),
            'error_urls': self.error_urls
        }

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2)
        
        if is_final:
            status_msg = "stopped" if self.stop_requested else "completed"
            self.logger.info(f"Crawl {status_msg}. Discovered {len(self.visited_urls)} URLs, scraped {self.total_pages_scraped} pages.")
            self.logger.info(f"Total unique links found: {len(self.all_links)}")
            self.logger.info(f"Total direct document links found: {len(self.document_links)}")
            self.logger.info(f"Total 404 URLs found: {len(self.not_found_urls)}")
            self.logger.info(f"Total URLs with errors: {len(self.error_urls)}")
            self.logger.info(f"Results saved to {self.output_file}")
            self.logger.info(f"Total execution time: {time.time() - self.start_time:.2f} seconds")
    
    def update_status(self):
        if self.stop_requested or self.stop_event.is_set():
            current_status = "stopped"
        else:
            current_status = "running"
            
        self.progress = self.calculate_progress()
        
        self.status = {
            "status": current_status,
            "links_found": self.total_links_processed,
            "pages_scraped": self.total_pages_scraped,
            "progress": self.progress,
            "execution_time_seconds": time.time() - self.start_time,
            "queue_size": len(self.url_queue),
            "frontier_size": len(self.frontier),
            "inactive_time": self.inactive_time,
            "was_stopped": self.stop_requested  
        }
        
        for callback in self.status_callbacks:
            try:
                callback(self.status)
            except Exception as e:
                self.logger.error(f"Error in status callback: {str(e)}")
                
        return self.status
    
    def start(self):
        self.status = "running"
        
        pakistan_time = datetime.now(pytz.timezone("Asia/Karachi"))
        formatted_time = pakistan_time.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Starting Apollo at {formatted_time} (Pakistan Time)")

        for i in range(self.num_workers):
            worker = threading.Thread(target=self.process_url, args=(i,))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
        
        try:
            while not self.stop_event.is_set() and not self.stop_requested:
                if (self.initial_processing_done.is_set() and 
                    self.links_queue.empty() and 
                    self.active_worker_count._value == self.num_workers):
                    
                    self.logger.info(f"Queue empty and all workers idle ({self.active_worker_count._value}/{self.num_workers}) - crawl complete")
                    break

                with self.last_activity_lock:
                    current_time = time.time()
                    self.inactive_time = current_time - self.last_activity_time

                if self.check_limits_reached():
                    if not self.stop_requested:  
                        self.stop_event.set()
                    self.logger.info("Limits reached - stopping crawl from main thread")
                    break

                current_time = time.time()
                if current_time - self.last_status_update >= self.status_update_interval:
                    self.update_status()
                    self.last_status_update = current_time

                if time.time() - self.last_idle_check >= self.idle_check_interval:
                    self.last_idle_check = time.time()
                    active = self.num_workers - self.active_worker_count._value
                    idle = self.active_worker_count._value
                    self.logger.debug(f"Worker status: {active} active, {idle} idle, queue size: {self.links_queue.qsize()}")

                if not self.stop_requested:
                    self.save_results(False)
                
                time.sleep(1)
        
        except KeyboardInterrupt:
            self.stop_event.set()
            self.stop_requested = True
            self.status = "interrupted"

        self.stop_event.set()
        self.logger.info("Stop event set - waiting for workers to finish")

        self.logger.info("Waiting for workers to finish...")
        for worker in self.workers:
            worker.join(timeout=5)

        active_count = sum(1 for w in self.workers if w.is_alive())
        if active_count > 0:
            self.logger.warning(f"{active_count} worker threads didn't terminate properly")

        self.save_results(True)
        end_time = time.time()
        total_time = end_time - self.start_time
        self.logger.info(f"Total time taken: {total_time:.2f} seconds")

        if self.stop_requested:
            self.status = "stopped"
        else:
            self.status = "completed"
        self.progress = 100.0 if not self.stop_requested else 95.0

        self.update_status()

        return self.get_results()
    
    def stop(self):
        if not self.stop_event.is_set() and not self.stop_requested:
            self.logger.info("Stopping crawler gracefully...")
            self.stop_requested = True  
            self.stop_event.set()
            self.status = "stopping"

            for worker in self.workers:
                worker.join(timeout=5)

            self.save_results(True)

            self.status = "stopped"
            self.progress = 95.0  

            self.update_status()
            
            return True
        return False
    
    def get_status(self):
        self.progress = self.calculate_progress()

        if self.stop_requested or self.stop_event.is_set():
            current_status = "stopped"
        else:
            current_status = "running"
        
        status = {
            'status': current_status,
            'progress': self.progress,
            'links_found': self.total_links_processed,
            'pages_scraped': self.total_pages_scraped,
            'execution_time_seconds': time.time() - self.start_time,
            'queue_size': len(self.url_queue),
            'frontier_size': len(self.frontier),
            'inactive_time': self.inactive_time,
            'was_stopped': self.stop_requested  
        }
        
        return status
    
    def get_results(self):
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
                'total_error_urls': len(self.error_urls),
                'was_stopped': self.stop_requested  
            },
            'all_links': list(self.all_links),
            'direct_document_links': list(self.document_links),
            '404_urls': list(self.not_found_urls),
            'error_urls': self.error_urls
        }
    
    def cleanup(self):
        self.visited_urls.clear()
        self.all_links.clear()
        self.document_links.clear()
        self.not_found_urls.clear()
        self.error_urls.clear()

        if DNS_AVAILABLE:
            self.robots_txt_cache.clear()
            self.dns_cache.clear()
        else:
            self.robots_txt_cache = {}
            self.dns_cache = {}

        while not self.links_queue.empty():
            try:
                self.links_queue.get_nowait()
                self.links_queue.task_done()
            except queue.Empty:
                break
                
        self.url_queue.clear()
        self.frontier.clear()

        self.total_links_processed = 0
        self.total_pages_scraped = 0

        with self.scraper_pool_lock:
            self.scraper_pool.clear()
        
        self.logger.info("Cleanup completed")
        
        return True
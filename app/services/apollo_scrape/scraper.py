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
from typing import Dict, List, Any, Optional, Tuple, Callable

class ClusterScraper:
    
    def __init__(
        self,
        json_file_path: str,
        output_dir: str = "scraped_content",
        metadata_dir: str = "document_metadata",
        expiry_days: Optional[int] = None,
        progress_update_interval: int = 2,  
        max_workers: int = 20  
    ):

        self.logger = self._setup_logger()
        self.json_file_path = json_file_path
        self.output_dir = output_dir
        self.metadata_dir = metadata_dir
        self.expiry_days = expiry_days
        self.progress_update_interval = progress_update_interval
        self.max_workers = max_workers
        self.clusters_data = self.load_clusters_json()
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
        self.counter_lock = threading.Lock()
        self.task_id = None
        self.task_manager = None
        self.progress_callback = None
        
        self.logger.info(f"ClusterScraper initialized with output_dir={output_dir}, metadata_dir={metadata_dir}, max_workers={max_workers}")
    
    def _setup_logger(self):
        logger = logging.getLogger("ClusterScraper")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def set_task_id(self, task_id: str) -> None:
        self.task_id = task_id
        self.logger.info(f"Task ID set to {task_id}")
    
    def set_progress_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        self.progress_callback = callback
        self.logger.info("Progress callback function set")
    
    def publish_progress(self, force: bool = False) -> None:
        should_update = (
            force or 
            (self.pages_processed % self.progress_update_interval == 0) or
            (self.total_pages > 0 and 
             (self.pages_processed / self.total_pages * 100) % self.progress_update_interval < 
             (1 / self.total_pages * 100))
        )
        
        if not should_update:
            return

        if self.total_pages > 0:
            self.progress = 5.0 + (self.pages_processed / self.total_pages * 55.0)

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

        if self.task_id:
            try:
                from app.utils.task_manager import task_manager
                self.task_manager = task_manager

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

        if self.progress_callback:
            try:
                self.progress_callback(progress_info)
            except Exception as e:
                self.logger.warning(f"Error in progress callback: {str(e)}")

        if self.pages_processed % 10 == 0 or force:
            self.logger.info(
                f"Progress: {self.pages_processed}/{self.total_pages} pages processed, "
                f"{self.pages_scraped} successful, {self.pages_failed} failed, "
                f"{self.progress:.1f}%"
            )
    
    def load_clusters_json(self) -> Optional[Dict[str, Any]]:
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
        if not self.clusters_data:
            return None

        for domain, domain_data in self.clusters_data.get("clusters", {}).items():
            if domain_data.get("id") == cluster_id:
                return domain_data

            for sub_cluster in domain_data.get("clusters", []):
                if sub_cluster.get("id") == cluster_id:
                    return sub_cluster
        
        self.logger.warning(f"Cluster with ID '{cluster_id}' not found.")
        return None
    
    def get_scraper(self):
        return cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
    
    def fetch_page(self, url: str, retry_attempts: int = 3) -> Optional[Any]:
        self.current_url = url
        
        for attempt in range(retry_attempts + 1):
            try:
                scraper = self.get_scraper()
                response = scraper.get(url, allow_redirects=True, timeout=30)

                final_url = response.url
                if "/404" in final_url or (final_url != url and ("not-found" in final_url or "error" in final_url)):
                    self.logger.warning(f"URL redirected to possible 404 page: {url} -> {final_url}")
                    response.status_code = 404
                
                return response
                
            except Exception as e:
                if attempt < retry_attempts:
                    self.logger.warning(f"Error fetching {url} (attempt {attempt+1}/{retry_attempts+1}): {str(e)}. Retrying...")
                    time.sleep(2)  
                else:
                    self.logger.error(f"Failed to fetch {url} after {retry_attempts+1} attempts: {str(e)}")
                    return None
        
        return None
    
    def parse_and_convert_to_markdown(self, html: str) -> Tuple[str, str, str]:
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # # # UBL
            # for tag in soup.find_all(['header', 'footer', 'nav', 'aside', 'script', 'style', 'div'],
            # class_=['mobile-login-field-small-wrapper','sub-page-links-wrapper','header-main-subpages','related-links-wrapper','content-wrapper','mobile-header-main', 'mm-header-nav-links','top-bar','login-field-small-wrapper-subpages','form-small-wrapper','side-nav-inner-page','footer-wrapper','mobile-copyrights-wrapper','privacy-links-wrapper','bread-crums-wrapper','dcp-form']):
            #     tag.decompose()



            # # FBL
            for tag in soup.find_all(['header', 'footer', 'nav', 'aside', 'script', 'style', 'div', 'button','a','section'],
            class_=['top-header','desk-header','mobile-header','breadcrumbs-bx','top-footer','middle-footer fb-footer','lowerfooter','nav-link','standard-btn mt-3','tab-sec-1 alpha importent-sec']):
                tag.decompose()

            # #BAFL
            # for tag in soup.find_all(['header', 'footer', 'nav', 'aside', 'script', 'style', 'div'],
            # class_=['col-lg-2 col-md-2 col-xs-2 col-sm-2 no-padding headerDiv','morph-menu-wrapper','phNumber',
            #         'show-on-hover topBarMenu portal-button','menu-premier-quick-links-container','mobile-nav','quickContact paddingSidemenuDefault',
            #         'pum-container popmake theme-2087406 pum-responsive pum-responsive-small responsive size-small',
            #         'pum-container popmake theme-2087405 pum-responsive pum-responsive-medium responsive size-medium',
            #         'pum-content popmake-content',' col-sm-10 no-padding classForRes','fontEm13 normalFont marginTop0',
            #         'col-sm-10 no-padding','countrySelect clearfix','col-sm-2 no-padding',' col-sm-10 no-padding classForRes'
            # ]):
                tag.decompose()

            for img in soup.find_all('img'):
                img.decompose()

            for figure in soup.find_all('figure'):
                figure.decompose()

            for picture in soup.find_all('picture'):
                picture.decompose()

            for svg in soup.find_all('svg'):
                svg.decompose()

            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                if heading.get_text(strip=True).lower() == "apply now":
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
                        for elem in heading.find_next_siblings():
                            if elem.name == 'form' or ('form' in elem.get('class', '')):
                                elem.decompose()
                        heading.decompose()

            content = soup.find_all(['article', 'section', 'main', 'div', 'main id', 'p'], 
                                   class_=['content', 'article-body', 'main-content',  'show', 
                                          'main-heading','tab-content inner-txt-bx','container'])
            
            if not content:
                content = soup.body
            
            if not content:
                self.logger.warning("No usable content found.")
                return "", "", ""

            title_tag = soup.find('title')
            if title_tag and title_tag.string:
                page_title = title_tag.string.strip()
            else:
                h1_tag = soup.find('h1')
                if h1_tag:
                    page_title = h1_tag.get_text(strip=True)
                else:
                    page_title = "untitled"

            clean_title = re.sub(r'[^\w\s-]', '', page_title).strip()
            clean_title = re.sub(r'[-\s]+', '-', clean_title)

            if not clean_title:
                clean_title = "untitled-content"

            markdown = md(str(content), heading_style="ATX")

            markdown = re.sub(r'!\[.*?\]\(.*?\)', '', markdown)

            markdown = re.sub(r'https?://\S+\.(jpg|jpeg|png|gif|svg|webp)(\?\S+)?', '', markdown, flags=re.IGNORECASE)
            
            return markdown, clean_title, page_title
            
        except Exception as e:
            self.logger.error(f"Error parsing HTML: {str(e)}")
            self.logger.error(traceback.format_exc())
            return "", "", ""
    
    def determine_source_from_url(self, url: str) -> str:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        if 'facebook.com' in domain or 'fb.com' in domain:
            return 'facebook'
        else:
            return 'website'
    
    def generate_document_id(self, content: str) -> str:
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
        parsed_url = url.split("://", 1)
        if len(parsed_url) < 2:
            return "unknown"
        
        domain_and_path = parsed_url[1].split("/", 1)
        domain = domain_and_path[0]
        dir_path = os.path.join(self.output_dir, domain)

        if len(domain_and_path) > 1 and domain_and_path[1]:
            path_parts = domain_and_path[1].split("/")
            for part in path_parts[:-1]:  
                if part:
                    dir_path = os.path.join(dir_path, part)
        
        return dir_path
    
    def process_url(self, url: str, cluster_id: str, expiry_date: Optional[str]) -> Dict[str, Any]:
        result = {
            "url": url,
            "success": False,
            "error": None
        }
        
        try:
            self.logger.info(f"Scraping: {url}")

            response = self.fetch_page(url)

            if response is None:
                result["error"] = "Failed to fetch page"
                with self.counter_lock:
                    self.pages_failed += 1
                    self.pages_processed += 1
                    self.publish_progress()
                return result

            if response.status_code != 200:
                result["error"] = f"Status code: {response.status_code}"
                with self.counter_lock:
                    self.pages_failed += 1
                    self.pages_processed += 1
                    self.publish_progress()
                return result

            html = response.text
            markdown_content, filename_base, document_title = self.parse_and_convert_to_markdown(html)
            
            if markdown_content:
                dir_path = self.url_to_directory_path(url)
                os.makedirs(dir_path, exist_ok=True)
                
                file_path = os.path.join(dir_path, f"{filename_base}.md")
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(markdown_content)
                
                self.logger.info(f"Content saved to: {file_path}")

                self.create_metadata_file(
                    self.metadata_dir,
                    filename_base,
                    document_title,
                    url,
                    markdown_content,
                    expiry_date
                )

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
        if task_id:
            self.set_task_id(task_id)
        
        if callback:
            self.set_progress_callback(callback)

        self.start_time = time.time()
        self.status = "initializing"
        self.progress = 0.0
        self.pages_scraped = 0
        self.pages_failed = 0
        self.pages_processed = 0
        self.total_pages = 0
        self.error = None  
        self.publish_progress(force=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)
        expiry_date = None
        if self.expiry_days is not None:
            expiry_date = (datetime.now() + timedelta(days=self.expiry_days)).strftime('%Y-%m-%d')
        self.status = "counting_pages"
        self.publish_progress(force=True)
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

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures_to_url = {
                    executor.submit(self.process_url, url, cluster_id, expiry_date): url
                    for url in urls
                }

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

                        cluster_results["urls_failed"] += 1
                        cluster_results["errors"].append({
                            "url": url,
                            "error": f"Thread execution error: {str(e)}"
                        })

                        with self.counter_lock:
                            self.pages_failed += 1
                            self.pages_processed += 1
                            self.publish_progress()

            results["clusters_scraped"].append(cluster_results)

        results["pages_scraped"] = self.pages_scraped
        results["pages_failed"] = self.pages_failed
        results["total_pages"] = self.total_pages
        results["execution_time_seconds"] = time.time() - self.start_time

        self.status = "completed"
        self.progress = 60.0  
        self.publish_progress(force=True)
        
        self.logger.info(
            f"Scraping completed. Scraped {self.pages_scraped} pages from {len(results['clusters_scraped'])} clusters."
        )
        
        return results
    
    def list_available_clusters(self) -> List[Dict[str, Any]]:
        if not self.clusters_data:
            return []
        
        clusters_info = []

        for domain, domain_data in self.clusters_data.get("clusters", {}).items():
            clusters_info.append({
                "id": domain_data.get("id"),
                "name": domain,
                "type": "domain",
                "url_count": domain_data.get("count", 0)
            })

            for sub_cluster in domain_data.get("clusters", []):
                clusters_info.append({
                    "id": sub_cluster.get("id"),
                    "name": f"{domain} - {sub_cluster.get('path', 'unknown-path')}",
                    "type": "path",
                    "url_count": sub_cluster.get("url_count", 0)
                })
        
        return clusters_info
    
    def get_status(self) -> Dict[str, Any]:
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
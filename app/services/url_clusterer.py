import re
from urllib.parse import urlparse
from collections import defaultdict
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Set, Tuple
from app.models.database.database_models import ProcessedLinks

class URLClusterer:
    def __init__(
        self,
        crawl_result_id: str,
        task_id: str,
        min_cluster_size: int = 2,
        path_depth: int = 2,
        similarity_threshold: float = 0.5,
        num_workers: int = 20 
    ):
        self.logger = self._setup_logger()
        self.crawl_result_id = crawl_result_id
        self.task_id = task_id
        self.min_cluster_size = min_cluster_size
        self.path_depth = path_depth
        self.similarity_threshold = similarity_threshold
        self.num_workers = num_workers
        self.lock = threading.Lock()
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.logger.info(f"URLClusterer initialized for crawl_result_id: {crawl_result_id} with min_cluster_size={min_cluster_size}, path_depth={path_depth}, similarity_threshold={similarity_threshold}, num_workers={num_workers}")
    
    def _setup_logger(self):
        logger = logging.getLogger("URLClusterer")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    async def load_bank_links_from_database(self) -> List[str]:
        """Load bank links from database instead of file."""
        try:
            processed_links = await ProcessedLinks.find_one(ProcessedLinks.crawl_result_id == self.crawl_result_id)
            
            if not processed_links:
                self.logger.error(f"No processed links found for crawl_result_id: {self.crawl_result_id}")
                return []
            
            bank_links = processed_links.bank_links
            self.logger.info(f"Loaded {len(bank_links)} bank links from database")
            return bank_links
            
        except Exception as e:
            self.logger.error(f"Error loading bank links from database: {str(e)}")
            return []
    
    def extract_url_components(self, url: str) -> Dict[str, Any]:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            path = parsed.path.rstrip('/')
            path_parts = [p for p in path.split('/') if p]
            
            if self.path_depth > 0:
                path_parts = path_parts[:self.path_depth]
            
            return {
                'full_url': url,
                'domain': domain,
                'path_parts': path_parts,
                'path': '/' + '/'.join(path_parts) if path_parts else '/',
                'query': parsed.query
            }
        except Exception as e:
            self.logger.warning(f"Error parsing URL {url}: {str(e)}")
            return {
                'full_url': url,
                'domain': '',
                'path_parts': [],
                'path': '/',
                'query': ''
            }
    
    def extract_url_batch(self, batch: List[str], batch_id: int) -> List[Dict[str, Any]]:
        self.logger.debug(f"Processing batch {batch_id} with {len(batch)} URLs")
        components = []
        for url in batch:
            components.append(self.extract_url_components(url))
        
        return components
    
    def parallel_extract_components(self, urls: List[str], batch_size: int = 500) -> List[Dict[str, Any]]:
        batches = []
        for i in range(0, len(urls), batch_size):
            batches.append((i // batch_size, urls[i:i + batch_size]))
        
        self.logger.info(f"Created {len(batches)} batches for URL component extraction")
        all_components = []
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {
                executor.submit(self.extract_url_batch, batch, batch_id): batch_id
                for batch_id, batch in batches
            }

            for i, future in enumerate(as_completed(futures)):
                try:
                    batch_components = future.result()
                    all_components.extend(batch_components)
                    self.progress = min(30.0, 10.0 + (i / len(batches) * 20.0))
                    self.logger.debug(f"Component extraction progress: {self.progress:.1f}%")
                    
                except Exception as e:
                    batch_id = futures[future]
                    self.logger.error(f"Error processing batch {batch_id}: {str(e)}")
        
        return all_components
    
    def cluster_by_domain(self, url_components: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        domain_clusters = defaultdict(list)
        
        for url_comp in url_components:
            domain_clusters[url_comp['domain']].append(url_comp)
        
        return domain_clusters
    
    def _path_similarity(self, path1: str, path2: str) -> float:
        parts1 = path1.split('/')
        parts2 = path2.split('/')
        common = 0
        for i in range(min(len(parts1), len(parts2))):
            if parts1[i] == parts2[i]:
                common += 1
            else:
                break

        total_unique_parts = len(set(parts1 + parts2))
        if total_unique_parts == 0:
            return 1.0  
        
        return common / total_unique_parts
    
    def _common_prefix(self, path1: str, path2: str) -> str:
        parts1 = path1.split('/')
        parts2 = path2.split('/')
        common_parts = []
        for i in range(min(len(parts1), len(parts2))):
            if parts1[i] == parts2[i]:
                common_parts.append(parts1[i])
            else:
                break
        
        return '/'.join(common_parts)
    
    def cluster_by_path_prefix(
        self, 
        domain_urls: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        path_clusters = defaultdict(list)

        for url_comp in domain_urls:
            path_clusters[url_comp['path']].append(url_comp)

        merged_clusters = defaultdict(list)
        processed_paths: Set[str] = set()
        sorted_paths = sorted(path_clusters.keys(), key=len)
        
        for path in sorted_paths:
            if path in processed_paths:
                continue
            
            current_cluster = path_clusters[path]
            merged_cluster = current_cluster.copy()
            pattern = path

            for other_path in sorted_paths:
                if other_path == path or other_path in processed_paths:
                    continue

                if (path.startswith(other_path + '/') or
                    other_path.startswith(path + '/') or
                    self._path_similarity(path, other_path) >= self.similarity_threshold):
                    
                    merged_cluster.extend(path_clusters[other_path])
                    processed_paths.add(other_path)

                    pattern = self._common_prefix(pattern, other_path)
            
            if len(merged_cluster) >= self.min_cluster_size:
                pattern_name = pattern if pattern else '/'
                if pattern == '/':
                    pattern_name = '/[ROOT]'
                
                merged_clusters[pattern_name] = merged_cluster
                processed_paths.add(path)

        for path, cluster in path_clusters.items():
            if path not in processed_paths and len(cluster) >= self.min_cluster_size:
                merged_clusters[path] = cluster
        
        return merged_clusters
    
    def process_domain(self, domain: str, domain_urls: List[Dict[str, Any]]) -> Tuple[str, Dict[str, List[Dict[str, Any]]]]:
        path_clusters = self.cluster_by_path_prefix(domain_urls)
        self.logger.debug(f"Domain {domain}: Found {len(path_clusters)} path clusters")
        
        return domain, path_clusters
    
    def parallel_domain_clustering(self, domain_clusters: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        domain_path_clusters = {}
        domains = list(domain_clusters.items())
        
        self.logger.info(f"Processing {len(domains)} domains in parallel")
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {
                executor.submit(self.process_domain, domain, urls): domain
                for domain, urls in domains
            }
            
            for i, future in enumerate(as_completed(futures)):
                try:
                    domain, path_clusters = future.result()
                    
                    if path_clusters: 
                        domain_path_clusters[domain] = path_clusters

                    self.progress = 50.0 + min(30.0, (i / len(domains) * 30.0))
                    
                except Exception as e:
                    domain = futures[future]
                    self.logger.error(f"Error processing domain {domain}: {str(e)}")
        
        return domain_path_clusters
    
    def prepare_clusters_for_output(
        self, 
        domain_path_clusters: Dict[str, Dict[str, List[Dict[str, Any]]]]
    ) -> Dict[str, Dict[str, Any]]:
        formatted_clusters = {}
        domain_id_counter = 1
        
        for domain, path_clusters in domain_path_clusters.items():
            domain_id = str(domain_id_counter)
            domain_formatted = {
                'id': domain_id,
                'count': sum(len(urls) for urls in path_clusters.values()),
                'clusters': []
            }

            for sub_id, (pattern, urls) in enumerate(path_clusters.items(), start=1):
                cluster_id = f"{domain_id}.{sub_id}"
                domain_formatted['clusters'].append({
                    'id': cluster_id,
                    'path': pattern,
                    'url_count': len(urls),
                    'urls': sorted([u['full_url'] for u in urls])
                })
            
            formatted_clusters[domain] = domain_formatted
            domain_id_counter += 1
        
        return formatted_clusters
    
    def generate_cluster_summary(self, clusters: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
        domain_count = len(clusters)
        total_clusters = sum(len(domain['clusters']) for domain in clusters.values())
        total_urls = sum(
            cluster['url_count']
            for domain in clusters.values()
            for cluster in domain['clusters']
        )
        
        return {
            'total_domains': domain_count,
            'total_clusters': total_clusters,
            'total_urls': total_urls
        }
    
    async def cluster(self) -> Dict[str, Any]:
        import time
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        
        self.logger.info(f"Starting URL clustering for crawl_result_id: {self.crawl_result_id}")
        bank_links = await self.load_bank_links_from_database()
        
        if not bank_links:
            self.logger.error("No bank links found to cluster")
            self.status = "error"

            empty_result = {
                "summary": {
                    "total_domains": 0,
                    "total_clusters": 0,
                    "total_urls": 0
                },
                "clusters": {}
            }
            
            return empty_result
        
        self.logger.info(f"Loaded {len(bank_links)} bank links for clustering")
        self.progress = 10.0

        self.logger.info("Extracting URL components in parallel")
        url_components = self.parallel_extract_components(bank_links)
        self.progress = 30.0

        self.logger.info("Grouping URLs by domain")
        domain_clusters = self.cluster_by_domain(url_components)
        self.logger.info(f"Grouped URLs into {len(domain_clusters)} domains")
        self.progress = 50.0

        self.logger.info("Clustering URLs by path within each domain (parallel)")
        domain_path_clusters = self.parallel_domain_clustering(domain_clusters)
        self.progress = 80.0

        self.logger.info("Formatting clusters for output")
        formatted_clusters = self.prepare_clusters_for_output(domain_path_clusters)
        self.progress = 90.0

        summary = {
            'total_domains': len(formatted_clusters),
            'total_clusters': sum(len(domain['clusters']) for domain in formatted_clusters.values()),
            'total_urls': len(bank_links)
        }

        result = {
            'summary': summary,
            'clusters': formatted_clusters
        }

        execution_time = time.time() - self.start_time
        self.logger.info(f"Clustering completed in {execution_time:.2f} seconds")

        self.status = "completed"
        self.progress = 100.0
        
        return result
    
    def get_status(self) -> Dict[str, Any]:
        import time
        return {
            'status': self.status,
            'progress': self.progress,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0
        }
import json
import re
from urllib.parse import urlparse
from collections import defaultdict
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Set, Tuple
from app.controllers.apollo_scrape.user_pref_controller import UserPreferenceController

class URLClusterer:
    def __init__(
        self,
        input_file: str = "categorized_links.json",
        output_file: str = "clustered_links.json",
        min_cluster_size: int = 2,
        path_depth: int = 2,
        similarity_threshold: float = 0.5,
        num_workers: int = 20 
    ):
        self.logger = self._setup_logger()
        self.input_file = input_file
        self.output_file = output_file
        self.min_cluster_size = min_cluster_size
        self.path_depth = path_depth
        self.similarity_threshold = similarity_threshold
        self.num_workers = num_workers
        self.lock = threading.Lock()
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.existing_urls: Set[str] = set() 
        self.logger.info(f"URLClusterer initialized with min_cluster_size={min_cluster_size}, path_depth={path_depth}, similarity_threshold={similarity_threshold}, num_workers={num_workers}")
    
    def _setup_logger(self):
        logger = logging.getLogger("URLClusterer")
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    async def fetch_existing_clustered_urls(self) -> Set[str]:
        try:
            self.logger.info("Fetching existing user preferences...")
            user_preference = await UserPreferenceController.get_user_preference()
            
            if not user_preference or not user_preference.clusters:
                self.logger.info("No existing user preferences or clusters found")
                return set()
            
            existing_urls = set()
            clusters_data = user_preference.clusters
            
            self.logger.debug(f"Clusters data structure: {type(clusters_data)}")

            for cluster_name, urls in clusters_data.items():
                self.logger.debug(f"Processing cluster '{cluster_name}' with {type(urls)} data")
                
                if isinstance(urls, list):
                    for url in urls:
                        if isinstance(url, str):
                            existing_urls.add(url)
                            self.logger.debug(f"Added URL from cluster '{cluster_name}': {url}")
                
                elif isinstance(urls, dict):
                    if 'clusters' in urls:
                        for cluster in urls['clusters']:
                            if isinstance(cluster, dict) and 'urls' in cluster:
                                for url in cluster['urls']:
                                    if isinstance(url, str):
                                        existing_urls.add(url)
                                        self.logger.debug(f"Added URL from hierarchical cluster: {url}")
                    else:
                        self._extract_urls_from_dict(urls, existing_urls, cluster_name)
                                
            self.logger.info(f"Found {len(existing_urls)} URLs in existing clusters")
            if existing_urls:
                self.logger.debug(f"Sample existing URLs: {list(existing_urls)[:5]}")
            return existing_urls
            
        except Exception as e:
            self.logger.error(f"Error fetching existing clustered URLs: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return set()
    
    def _extract_urls_from_dict(self, data: dict, existing_urls: set, context: str = ""):
        for key, value in data.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and self._is_url(item):
                        existing_urls.add(item)
                        self.logger.debug(f"Added URL from dict context '{context}.{key}': {item}")
                    elif isinstance(item, dict):
                        self._extract_urls_from_dict(item, existing_urls, f"{context}.{key}")
            elif isinstance(value, dict):
                self._extract_urls_from_dict(value, existing_urls, f"{context}.{key}")
            elif isinstance(value, str) and self._is_url(value):
                existing_urls.add(value)
                self.logger.debug(f"Added URL from dict context '{context}.{key}': {value}")
    
    def _is_url(self, text: str) -> bool:
        return text.startswith(('http://', 'https://')) and '.' in text
    
    def filter_new_urls(self, bank_links: List[str]) -> List[str]:
        if not self.existing_urls:
            self.logger.info("No existing URLs to filter, processing all links")
            return bank_links
        
        new_urls = []
        skipped_count = 0
        
        for url in bank_links:
            if url not in self.existing_urls:
                new_urls.append(url)
            else:
                skipped_count += 1
        
        self.logger.info(f"Filtered {skipped_count} already clustered URLs, processing {len(new_urls)} new URLs")
        return new_urls

    def normalize_cluster_name(self, cluster_name):
        if not cluster_name:
            return ""

        normalized = re.sub(r'[-\s]+', ' ', cluster_name.lower())
        return normalized

    def normalize_path_display(self, path):
        if not path or path == '/':
            return '/'

        parts = path.split('/')
        normalized_parts = []

        for part in parts:
            if part:  
                normalized_part = re.sub(r'-+', ' ', part)
                normalized_parts.append(normalized_part)

        return '/' + '/'.join(normalized_parts) if normalized_parts else '/'
    
    def load_links(self) -> List[str]:
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'bank_links' not in data:
                self.logger.error(f"'bank_links' key not found in {self.input_file}")
                return []
            
            return data['bank_links']
        except FileNotFoundError:
            self.logger.error(f"File not found: {self.input_file}")
            return []
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON format in file: {self.input_file}")
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
    
    def merge_normalized_clusters(self, formatted_clusters):
        merged_clusters = {}

        for domain, domain_data in formatted_clusters.items():
            normalized_map = {}

            for cluster in domain_data['clusters']:
                cluster_name = f"{domain}{cluster['path']}"
                normalized_name = self.normalize_cluster_name(cluster_name)

                if normalized_name in normalized_map:
                    existing_cluster = normalized_map[normalized_name]
                    existing_cluster['urls'].extend(cluster['urls'])
                    existing_cluster['original_paths'].append(cluster['path'])
                    self.logger.info(f"Merged cluster '{cluster['path']}' into existing normalized cluster '{normalized_name}'")
                else:
                    normalized_map[normalized_name] = {
                        'id': cluster['id'],
                        'path': cluster['path'],
                        'urls': cluster['urls'].copy(),
                        'original_paths': [cluster['path']]
                    }

            merged_domain_clusters = []
            for normalized_name, cluster_data in normalized_map.items():
                unique_urls = []
                seen_urls = set()

                for url in cluster_data['urls']:
                    if url not in seen_urls:
                        unique_urls.append(url)
                        seen_urls.add(url)

                duplicates_removed = len(cluster_data['urls']) - len(unique_urls)
                if duplicates_removed > 0:
                    self.logger.info(f"Removed {duplicates_removed} duplicate URLs from cluster '{cluster_data['path']}'")

                merged_cluster = {
                    'id': cluster_data['id'],
                    'path': self.normalize_path_display(cluster_data['path']),
                    'normalized_name': normalized_name,
                    'url_count': len(unique_urls),
                    'urls': sorted(unique_urls),
                    'original_paths': cluster_data['original_paths']
                }

                merged_domain_clusters.append(merged_cluster)

            merged_clusters[domain] = {
                'id': domain_data['id'],
                'count': sum(cluster['url_count'] for cluster in merged_domain_clusters),
                'clusters': merged_domain_clusters
            }

        return merged_clusters
    
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
            'total_urls': total_urls,
            'skipped_urls': len(self.existing_urls)
        }
    
    async def cluster(self) -> Dict[str, Any]:
        import time
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        
        self.logger.info("Starting URL clustering")

        self.existing_urls = await self.fetch_existing_clustered_urls()
        self.progress = 5.0

        bank_links = self.load_links()
        if not bank_links:
            self.logger.error("No bank links found to cluster")
            self.status = "error"

            empty_result = {
                "summary": {
                    "total_domains": 0,
                    "total_clusters": 0,
                    "total_urls": 0,
                    "skipped_urls": len(self.existing_urls)
                },
                "clusters": {}
            }

            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(empty_result, f, indent=2)
                
            self.logger.info(f"Empty results saved to {self.output_file}")
            return empty_result
        
        self.logger.info(f"Loaded {len(bank_links)} bank links for clustering")

        new_bank_links = self.filter_new_urls(bank_links)
        
        if not new_bank_links:
            self.logger.info("All URLs are already clustered, no new clusters to create")
            self.status = "completed"
            self.progress = 100.0
            
            empty_result = {
                "summary": {
                    "total_domains": 0,
                    "total_clusters": 0,
                    "total_urls": 0,
                    "skipped_urls": len(self.existing_urls)
                },
                "clusters": {}
            }
            
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(empty_result, f, indent=2)
                
            return empty_result
        
        self.progress = 10.0

        self.logger.info("Extracting URL components in parallel")
        url_components = self.parallel_extract_components(new_bank_links)
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

        self.logger.info("Merging clusters with normalized names and removing duplicates...")
        merged_clusters = self.merge_normalized_clusters(formatted_clusters)
        self.progress = 90.0

        summary = self.generate_cluster_summary(merged_clusters)

        result = {
            'summary': summary,
            'clusters': merged_clusters
        }

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        
        execution_time = time.time() - self.start_time
        self.logger.info(f"Clustering completed in {execution_time:.2f} seconds. Results saved to {self.output_file}")
        self.logger.info(f"Summary: {summary}")

        self.status = "completed"
        self.progress = 100.0
        
        return result
    
    def get_status(self) -> Dict[str, Any]:
        import time
        return {
            'status': self.status,
            'progress': self.progress,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0,
            'existing_urls_count': len(self.existing_urls)
        }
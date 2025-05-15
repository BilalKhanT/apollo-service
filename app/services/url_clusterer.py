import json
import re
from urllib.parse import urlparse
from collections import defaultdict
import logging
import os
from typing import Dict, List, Any, Optional, Set, Tuple

class URLClusterer:
    """
    A class to cluster URLs by domain and path patterns.
    """
    
    def __init__(
        self,
        input_file: str = "categorized_links.json",
        output_file: str = "clustered_links.json",
        min_cluster_size: int = 2,
        path_depth: int = 2,
        similarity_threshold: float = 0.5
    ):
        """
        Initialize the URL clusterer.

        Args:
            input_file: Path to categorized_links.json file
            output_file: Path to save clustered links
            min_cluster_size: Minimum number of links in a cluster
            path_depth: Maximum depth of path components to consider
            similarity_threshold: Threshold for URL path similarity
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.input_file = input_file
        self.output_file = output_file
        self.min_cluster_size = min_cluster_size
        self.path_depth = path_depth
        self.similarity_threshold = similarity_threshold
        
        # Status tracking
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
    
    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("URLClusterer")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def load_links(self) -> List[str]:
        """Load bank links from the categorized JSON file."""
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
        """Extract domain and path components from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            
            # Remove trailing slash and split path
            path = parsed.path.rstrip('/')
            path_parts = [p for p in path.split('/') if p]
            
            # Limit path depth if specified
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
    
    def cluster_by_domain(self, url_components: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group URLs by domain."""
        domain_clusters = defaultdict(list)
        
        for url_comp in url_components:
            domain_clusters[url_comp['domain']].append(url_comp)
        
        return domain_clusters
    
    def cluster_by_path_prefix(
        self, 
        domain_urls: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Cluster URLs within a domain by common path prefixes.
        Returns a dictionary of path patterns to URLs that match that pattern.
        """
        path_clusters = defaultdict(list)
        
        # First, group by exact path
        for url_comp in domain_urls:
            path_clusters[url_comp['path']].append(url_comp)
        
        # Then, attempt to merge similar paths
        merged_clusters = defaultdict(list)
        processed_paths: Set[str] = set()
        
        # Sort paths by length (ascending) to process shorter paths first
        sorted_paths = sorted(path_clusters.keys(), key=len)
        
        for path in sorted_paths:
            # Skip if this path has been merged into another cluster
            if path in processed_paths:
                continue
            
            current_cluster = path_clusters[path]
            merged_cluster = current_cluster.copy()
            pattern = path
            
            # Look for similar patterns to merge
            for other_path in sorted_paths:
                if other_path == path or other_path in processed_paths:
                    continue
                
                # Check if this is a subpath of the current path
                # or if they share significant common prefix
                if (path.startswith(other_path + '/') or
                    other_path.startswith(path + '/') or
                    self._path_similarity(path, other_path) >= self.similarity_threshold):
                    
                    merged_cluster.extend(path_clusters[other_path])
                    processed_paths.add(other_path)
                    
                    # Create a pattern that represents the common prefix
                    pattern = self._common_prefix(pattern, other_path)
            
            # Add to merged clusters if not empty and meets size threshold
            if len(merged_cluster) >= self.min_cluster_size:
                # Make pattern more descriptive
                pattern_name = pattern if pattern else '/'
                if pattern == '/':
                    pattern_name = '/[ROOT]'
                
                merged_clusters[pattern_name] = merged_cluster
                processed_paths.add(path)
        
        # Add any remaining unmerged clusters that meet the size threshold
        for path, cluster in path_clusters.items():
            if path not in processed_paths and len(cluster) >= self.min_cluster_size:
                merged_clusters[path] = cluster
        
        return merged_clusters
    
    def _path_similarity(self, path1: str, path2: str) -> float:
        """Calculate similarity between two paths."""
        # Split paths into components
        parts1 = path1.split('/')
        parts2 = path2.split('/')
        
        # Find common parts
        common = 0
        for i in range(min(len(parts1), len(parts2))):
            if parts1[i] == parts2[i]:
                common += 1
            else:
                break
        
        # Calculate similarity as ratio of common parts to total unique parts
        total_unique_parts = len(set(parts1 + parts2))
        if total_unique_parts == 0:
            return 1.0  # Both empty paths
        
        return common / total_unique_parts
    
    def _common_prefix(self, path1: str, path2: str) -> str:
        """Find the common prefix of two paths."""
        parts1 = path1.split('/')
        parts2 = path2.split('/')
        
        common_parts = []
        for i in range(min(len(parts1), len(parts2))):
            if parts1[i] == parts2[i]:
                common_parts.append(parts1[i])
            else:
                break
        
        return '/'.join(common_parts)
    
    def prepare_clusters_for_output(
        self, 
        domain_path_clusters: Dict[str, Dict[str, List[Dict[str, Any]]]]
    ) -> Dict[str, Dict[str, Any]]:
        """Format clusters for JSON output."""
        formatted_clusters = {}
        domain_id_counter = 1
        
        for domain, path_clusters in domain_path_clusters.items():
            domain_id = str(domain_id_counter)
            domain_formatted = {
                'id': domain_id,
                'count': sum(len(urls) for urls in path_clusters.values()),
                'clusters': []
            }
            
            # Add each path cluster with hierarchical ID
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
        """Generate a summary of all clusters."""
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
    
    def cluster(self) -> Dict[str, Any]:
        """Cluster URLs by domain and path patterns."""
        import time
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        
        self.logger.info("Starting URL clustering")
        
        # Load bank links
        bank_links = self.load_links()
        if not bank_links:
            self.logger.error("No bank links found to cluster")
            self.status = "error"
            
            # Return empty result structure with summary instead of empty dict
            empty_result = {
                "summary": {
                    "total_domains": 0,
                    "total_clusters": 0,
                    "total_urls": 0
                },
                "clusters": {}
            }
            
            # Save empty result to file
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(empty_result, f, indent=2)
                
            self.logger.info(f"Empty results saved to {self.output_file}")
            return empty_result
        
        self.logger.info(f"Loaded {len(bank_links)} bank links for clustering")
        self.progress = 10.0
        
        # Extract URL components
        url_components = [self.extract_url_components(url) for url in bank_links]
        self.progress = 30.0
        
        # Group by domain first
        domain_clusters = self.cluster_by_domain(url_components)
        self.logger.info(f"Grouped URLs into {len(domain_clusters)} domains")
        self.progress = 50.0
        
        # For each domain, cluster by path pattern
        domain_path_clusters = {}
        domains_count = len(domain_clusters)
        
        for i, (domain, domain_urls) in enumerate(domain_clusters.items()):
            path_clusters = self.cluster_by_path_prefix(domain_urls)
            if path_clusters:
                domain_path_clusters[domain] = path_clusters
                self.logger.info(f"Domain {domain}: {len(path_clusters)} path clusters")
            
            # Update progress
            self.progress = 50.0 + 30.0 * ((i + 1) / domains_count)
        
        # Format clusters for output
        formatted_clusters = self.prepare_clusters_for_output(domain_path_clusters)
        self.progress = 90.0
        
        # Generate summary
        summary = {
            'total_domains': len(formatted_clusters),
            'total_clusters': sum(len(domain['clusters']) for domain in formatted_clusters.values()),
            'total_urls': len(bank_links)
        }
        
        # Create final output
        result = {
            'summary': summary,
            'clusters': formatted_clusters
        }
        
        # Save to file
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        
        self.logger.info(f"Clustering completed. Results saved to {self.output_file}")
        
        # Update status
        self.status = "completed"
        self.progress = 100.0
        
        return result
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the clusterer."""
        import time
        return {
            'status': self.status,
            'progress': self.progress,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0
        }
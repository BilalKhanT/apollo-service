import json
import re
import os
import threading
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any

class YearExtractor:
    def __init__(
        self,
        input_file: str = "categorized_links.json",
        output_file: str = "clustered_by_year.json",
        num_workers: int = 20,  
        batch_size: int = 500  
    ):
        self.logger = self._setup_logger()
        self.input_file = input_file
        self.output_file = output_file
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.full_year_pattern = re.compile(r'(?:19|20)\d{2}')
        self.lock = threading.Lock()
        self.progress_lock = threading.Lock()
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.processed_count = 0
        self.logger.info(f"YearExtractor initialized with {num_workers} workers and batch_size={batch_size}")
    
    def _setup_logger(self):
        logger = logging.getLogger("YearExtractor")
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger
    
    def load_file_links(self) -> List[str]:
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'file_links' not in data:
                self.logger.error(f"'file_links' key not found in {self.input_file}")
                return []
            
            return data['file_links']
        except FileNotFoundError:
            self.logger.error(f"File not found: {self.input_file}")
            return []
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON format in file: {self.input_file}")
            return []
    
    def extract_from_filename(self, filename: str) -> Optional[str]:
        match = self.full_year_pattern.search(filename)
        if match:
            return match.group()
        return None
    
    def extract_from_query_params(self, query_string: str) -> Optional[str]:
        if not query_string:
            return None

        params = parse_qs(query_string)
        
        for param, values in params.items():
            for value in values:
                match = self.full_year_pattern.search(value)
                if match:
                    return match.group()
        
        return None
    
    def extract_from_path(self, path: str) -> Optional[str]:
        if not path:
            return None
        
        match = self.full_year_pattern.search(path)
        if match:
            return match.group()
        return None
    
    def extract(self, url: str) -> str:
        parsed_url = urlparse(url)

        filename = os.path.basename(parsed_url.path)
        year = self.extract_from_filename(filename)
        if year:
            return year

        year = self.extract_from_query_params(parsed_url.query)
        if year:
            return year

        year = self.extract_from_path(parsed_url.path)
        if year:
            return year

        return "No Year"
    
    def process_batch(self, batch_id: int, urls: List[str]) -> Dict[str, List[str]]:
        self.logger.debug(f"Processing batch {batch_id} with {len(urls)} URLs")

        local_clusters = defaultdict(list)
        
        for url in urls:
            year = self.extract(url)
            local_clusters[year].append(url)

        with self.progress_lock:
            self.processed_count += len(urls)
            total_links = getattr(self, 'total_links', 1)  
            self.progress = min(99.0, (self.processed_count / total_links) * 100)
        
        return local_clusters
    
    def merge_results(self, results: List[Dict[str, List[str]]]) -> Dict[str, List[str]]:
        merged = defaultdict(list)
        
        for result in results:
            for year, urls in result.items():
                merged[year].extend(urls)
        
        return merged
    
    def process(self) -> Dict[str, List[str]]:
        import time
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        self.processed_count = 0
        self.logger.info(f"Starting year extraction from {self.input_file}")

        file_links = self.load_file_links()
        if not file_links:
            self.logger.warning("No file links found to process")
            self.status = "completed"
            self.progress = 100.0
            
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=4)
                
            return {}
        
        self.total_links = len(file_links)
        self.logger.info(f"Loaded {self.total_links} file links for processing")
        batches = []
        for i in range(0, len(file_links), self.batch_size):
            batch_id = i // self.batch_size
            batch_urls = file_links[i:i+self.batch_size]
            batches.append((batch_id, batch_urls))
        
        self.logger.info(f"Created {len(batches)} batches for parallel processing")
        batch_results = []
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {
                executor.submit(self.process_batch, batch_id, urls): batch_id
                for batch_id, urls in batches
            }

            for future in as_completed(futures):
                try:
                    batch_result = future.result()
                    batch_results.append(batch_result)

                    if len(batch_results) % 5 == 0 or len(batch_results) == len(batches):
                        self.logger.info(f"Progress: {self.progress:.1f}% ({self.processed_count}/{self.total_links} URLs processed)")
                    
                except Exception as e:
                    batch_id = futures[future]
                    self.logger.error(f"Error processing batch {batch_id}: {str(e)}")

        clustered = self.merge_results(batch_results)
        year_counts = {year: len(urls) for year, urls in clustered.items()}
        self.logger.info(f"Year clusters: {year_counts}")

        if "No Year" in clustered and clustered["No Year"]:
            no_year_count = len(clustered["No Year"])
            self.logger.info(f"Files without detected year: {no_year_count}")

            if no_year_count > 0:
                examples = clustered["No Year"][:min(5, no_year_count)]
                self.logger.debug(f"Examples of 'No Year' links: {examples}")

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(clustered, f, indent=4)
        
        execution_time = time.time() - self.start_time
        self.logger.info(f"Year clustering completed in {execution_time:.2f} seconds. Results saved to {self.output_file}")
        self.status = "completed"
        self.progress = 100.0
        
        return clustered
    
    def get_status(self) -> Dict[str, Any]:
        import time
        return {
            'status': self.status,
            'progress': self.progress,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0
        }
import json
import re
import os
import threading
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any, Set
from app.controllers.apollo_scrape.user_pref_controller import UserPreferenceController

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
        self.existing_urls: Set[str] = set()  
        self.logger.info(f"YearExtractor initialized with {num_workers} workers and batch_size={batch_size}")
    
    def _setup_logger(self):
        logger = logging.getLogger("YearExtractor")
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger
    
    async def fetch_existing_year_clustered_urls(self) -> Set[str]:
        try:
            self.logger.info("Fetching existing year-clustered URLs from user preferences...")
            user_preference = await UserPreferenceController.get_user_preference()
            
            if not user_preference or not user_preference.years:
                self.logger.info("No existing user preferences or year clusters found")
                return set()
            
            existing_urls = set()
            years_data = user_preference.years
            
            self.logger.debug(f"Years data structure: {type(years_data)}")

            if isinstance(years_data, dict):
                for year, urls in years_data.items():
                    self.logger.debug(f"Processing year '{year}' with {type(urls)} data")
                    
                    if isinstance(urls, list):
                        for url in urls:
                            if isinstance(url, str) and self._is_url(url):
                                existing_urls.add(url)
                                self.logger.debug(f"Added URL from year '{year}': {url}")
                            elif isinstance(url, str):
                                self.logger.warning(f"Invalid URL format in year '{year}': {url}")
                    
                    elif isinstance(urls, dict):
                        self._extract_urls_from_year_dict(urls, existing_urls, year)
                    
                    else:
                        self.logger.warning(f"Unexpected data type for year '{year}': {type(urls)}")
            
            elif isinstance(years_data, list):
                self.logger.warning("Years data is a list, attempting to extract URLs")
                for item in years_data:
                    if isinstance(item, str) and self._is_url(item):
                        existing_urls.add(item)
            
            else:
                self.logger.warning(f"Unexpected years data type: {type(years_data)}")
            
            self.logger.info(f"Found {len(existing_urls)} URLs in existing year clusters")
            if existing_urls:
                self.logger.debug(f"Sample existing year URLs: {list(existing_urls)[:5]}")
            return existing_urls
            
        except Exception as e:
            self.logger.error(f"Error fetching existing year-clustered URLs: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return set()
    
    def _extract_urls_from_year_dict(self, data: dict, existing_urls: set, year: str = ""):
        for key, value in data.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and self._is_url(item):
                        existing_urls.add(item)
                        self.logger.debug(f"Added URL from year dict '{year}.{key}': {item}")
                    elif isinstance(item, dict):
                        self._extract_urls_from_year_dict(item, existing_urls, f"{year}.{key}")
            elif isinstance(value, dict):
                self._extract_urls_from_year_dict(value, existing_urls, f"{year}.{key}")
            elif isinstance(value, str) and self._is_url(value):
                existing_urls.add(value)
                self.logger.debug(f"Added URL from year dict '{year}.{key}': {value}")
    
    def _is_url(self, text: str) -> bool:
        return text.startswith(('http://', 'https://')) and '.' in text
    
    def filter_new_urls(self, file_links: List[str]) -> List[str]:
        if not self.existing_urls:
            self.logger.info("No existing URLs to filter, processing all links")
            return file_links
        
        new_urls = []
        skipped_count = 0
        
        for url in file_links:
            if url not in self.existing_urls:
                new_urls.append(url)
            else:
                skipped_count += 1
        
        self.logger.info(f"Filtered {skipped_count} already year-clustered URLs, processing {len(new_urls)} new URLs")
        return new_urls
    
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
    
    def generate_summary(self, clustered: Dict[str, List[str]]) -> Dict[str, Any]:
        """
        Generate a summary of the year clustering results
        """
        total_years = len(clustered)
        total_urls = sum(len(urls) for urls in clustered.values())
        year_counts = {year: len(urls) for year, urls in clustered.items()}
        
        return {
            'total_years': total_years,
            'total_urls': total_urls,
            'skipped_urls': len(self.existing_urls),
            'year_distribution': year_counts
        }
    
    async def process(self) -> Dict[str, Any]:
        import time
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        self.processed_count = 0
        self.logger.info(f"Starting year extraction from {self.input_file}")

        self.existing_urls = await self.fetch_existing_year_clustered_urls()
        self.progress = 5.0

        file_links = self.load_file_links()
        if not file_links:
            self.logger.warning("No file links found to process")
            self.status = "completed"
            self.progress = 100.0
            
            empty_result = {
                "summary": {
                    "total_years": 0,
                    "total_urls": 0,
                    "skipped_urls": len(self.existing_urls),
                    "year_distribution": {}
                },
                "years": {}
            }
            
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(empty_result, f, indent=4)
                
            return empty_result
        
        self.logger.info(f"Loaded {len(file_links)} file links for processing")

        new_file_links = self.filter_new_urls(file_links)
        
        if not new_file_links:
            self.logger.info("All URLs are already year-clustered, no new year clusters to create")
            self.status = "completed"
            self.progress = 100.0
            
            empty_result = {
                "summary": {
                    "total_years": 0,
                    "total_urls": 0,
                    "skipped_urls": len(self.existing_urls),
                    "year_distribution": {}
                },
                "years": {}
            }
            
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(empty_result, f, indent=4)
                
            return empty_result
        
        self.total_links = len(new_file_links)
        self.logger.info(f"Processing {self.total_links} new file links")
        self.progress = 10.0

        batches = []
        for i in range(0, len(new_file_links), self.batch_size):
            batch_id = i // self.batch_size
            batch_urls = new_file_links[i:i+self.batch_size]
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

        summary = self.generate_summary(clustered)
        
        self.logger.info(f"Year clustering summary: {summary}")

        if "No Year" in clustered and clustered["No Year"]:
            no_year_count = len(clustered["No Year"])
            self.logger.info(f"Files without detected year: {no_year_count}")

            if no_year_count > 0:
                examples = clustered["No Year"][:min(5, no_year_count)]
                self.logger.debug(f"Examples of 'No Year' links: {examples}")

        result = {
            "summary": summary,
            "years": dict(clustered)  
        }

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=4)
        
        execution_time = time.time() - self.start_time
        self.logger.info(f"Year clustering completed in {execution_time:.2f} seconds. Results saved to {self.output_file}")
        self.logger.info(f"Final summary: {summary}")
        
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
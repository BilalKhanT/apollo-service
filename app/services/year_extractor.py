import json
import re
import os
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
import logging
from typing import Dict, List, Optional, Any

class YearExtractor:
    """
    A class to extract years from file URLs and cluster files by year.
    """
    
    def __init__(
        self,
        input_file: str = "categorized_links.json",
        output_file: str = "clustered_by_year.json"
    ):
        """
        Initialize the year extractor.

        Args:
            input_file: Path to categorized_links.json file
            output_file: Path to save clustered by year file
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.input_file = input_file
        self.output_file = output_file
        
        # Compile the year pattern (1990-2099)
        self.full_year_pattern = re.compile(r'(?:19|20)\d{2}')
        
        # Status tracking
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
    
    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("YearExtractor")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def load_file_links(self) -> List[str]:
        """Load file links from the categorized JSON file."""
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
        """Extract year from filename only."""
        match = self.full_year_pattern.search(filename)
        if match:
            return match.group()
        return None
    
    def extract_from_query_params(self, query_string: str) -> Optional[str]:
        """Extract year from URL query parameters."""
        if not query_string:
            return None
        
        # Parse query parameters
        params = parse_qs(query_string)
        
        # Check each parameter value for a year
        for param, values in params.items():
            for value in values:
                match = self.full_year_pattern.search(value)
                if match:
                    return match.group()
        
        return None
    
    def extract_from_path(self, path: str) -> Optional[str]:
        """Extract year from the entire URL path."""
        if not path:
            return None
        
        match = self.full_year_pattern.search(path)
        if match:
            return match.group()
        return None
    
    def extract(self, url: str) -> str:
        """
        Extract year from URL using multiple methods.
        
        This function tries different extraction methods in the following order:
        1. From filename
        2. From query parameters
        3. From the entire path
        
        If no year is found, "No Year" is returned.
        """
        parsed_url = urlparse(url)
        
        # Try to extract from filename first
        filename = os.path.basename(parsed_url.path)
        year = self.extract_from_filename(filename)
        if year:
            return year
        
        # Try to extract from query parameters
        year = self.extract_from_query_params(parsed_url.query)
        if year:
            return year
        
        # Try to extract from the entire path
        year = self.extract_from_path(parsed_url.path)
        if year:
            return year
        
        # Return "No Year" as the fallback
        return "No Year"
    
    def cluster_by_year(self, file_links: List[str]) -> Dict[str, List[str]]:
        """
        Cluster file links by the year extracted from their URLs.
        
        Args:
            file_links: List of file URLs to process
            
        Returns:
            Dictionary with years as keys and lists of URLs as values
        """
        clusters = defaultdict(list)
        
        total_files = len(file_links)
        for i, url in enumerate(file_links):
            year = self.extract(url)
            clusters[year].append(url)
            
            # Update progress every 100 files
            if i % 100 == 0:
                self.progress = min(99.0, (i / total_files) * 100)
        
        return clusters
    
    def analyze_no_year_links(self, file_links: List[str]) -> List[str]:
        """
        Analyze URLs that couldn't be classified with a year.
        
        Args:
            file_links: List of file URLs to analyze
            
        Returns:
            List of URLs that didn't have a year detected
        """
        no_year_links = []
        
        for url in file_links:
            year = self.extract(url)
            if year == "No Year":
                no_year_links.append(url)
        
        return no_year_links
    
    def process(self) -> Dict[str, List[str]]:
        """
        Process file links and cluster them by year.
        
        Returns:
            Dictionary with years as keys and lists of URLs as values
        """
        import time
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        
        self.logger.info(f"Starting year extraction from {self.input_file}")
        
        # Load file links
        file_links = self.load_file_links()
        if not file_links:
            self.logger.warning("No file links found to process")
            self.status = "completed"
            self.progress = 100.0
            return {}
        
        self.logger.info(f"Loaded {len(file_links)} file links for processing")
        
        # Cluster by year
        clustered = self.cluster_by_year(file_links)
        
        # Log some statistics
        year_counts = {year: len(urls) for year, urls in clustered.items()}
        self.logger.info(f"Year clusters: {year_counts}")
        
        # Analyze "No Year" links if there are any
        if "No Year" in clustered and clustered["No Year"]:
            no_year_count = len(clustered["No Year"])
            self.logger.info(f"Files without detected year: {no_year_count}")
            
            # Log a few examples for debugging
            if no_year_count > 0:
                examples = clustered["No Year"][:min(5, no_year_count)]
                self.logger.debug(f"Examples of 'No Year' links: {examples}")
        
        # Save to file
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(clustered, f, indent=4)
        
        self.logger.info(f"Year clustering completed. Results saved to {self.output_file}")
        
        # Update status
        self.status = "completed"
        self.progress = 100.0
        
        return clustered
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the year extractor."""
        import time
        return {
            'status': self.status,
            'progress': self.progress,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0
        }
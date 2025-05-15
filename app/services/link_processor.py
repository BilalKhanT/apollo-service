import json
import re
import threading
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Set, Any, Optional

class LinkProcessor:
    """
    A class to process and categorize links found by the crawler.
    """
    
    def __init__(
        self,
        input_file: str = "all_links.json",
        output_file: str = "categorized_links.json",
        num_workers: int = 8,
        file_extensions: Optional[List[str]] = None,
        social_media_keywords: Optional[List[str]] = None,
        bank_keywords: Optional[List[str]] = None
    ):
        """
        Initialize the Link Processor.

        Args:
            input_file: Path to all_links.json file
            output_file: Path to save categorized links
            num_workers: Number of worker threads
            file_extensions: List of file extensions to identify files
            social_media_keywords: List of keywords to identify social media links
            bank_keywords: List of keywords to identify bank-related links
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.input_file = input_file
        self.output_file = output_file
        self.num_workers = num_workers
        
        # Set up categorization patterns
        # Default file extensions if none provided
        self.file_extensions = file_extensions or [
            'pdf', 'xls', 'xlsx', 'doc', 'docx', 'ppt', 'pptx',
            'csv', 'txt', 'rtf', 'zip', 'rar', 'tar', 'gz', 'xlsb'
        ]
        
        # Default social media keywords if none provided
        self.social_media_keywords = social_media_keywords or [
            'instagram', 'facebook', 'linkedin', 'twitter', 'tiktok',
            'youtube', 'apps.google', 'appstore', 'play.google', 'app.apple'
        ]
        
        # Default bank keywords if none provided
        self.bank_keywords = bank_keywords or ['bafl', 'falah']
        
        # Compile regex patterns for efficient matching
        self.file_pattern = re.compile(
            fr'\.({"|".join(self.file_extensions)})($|\?)',
            re.IGNORECASE
        )
        self.social_media_pattern = re.compile(
            fr'({"|".join(self.social_media_keywords)})',
            re.IGNORECASE
        )
        self.bank_pattern = re.compile(
            fr'({"|".join(self.bank_keywords)})',
            re.IGNORECASE
        )
        
        # Result containers with thread safety
        self.file_links: Set[str] = set()
        self.social_media_links: Set[str] = set()
        self.bank_links: Set[str] = set()
        self.misc_links: Set[str] = set()
        self.lock = threading.Lock()
        
        # Status tracking
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
    
    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("LinkProcessor")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def load_links(self) -> List[str]:
        """Load links from the JSON file."""
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'all_links' not in data:
                self.logger.error(f"Invalid JSON format: 'all_links' key not found in {self.input_file}")
                return []
            
            return data['all_links']
        except FileNotFoundError:
            self.logger.error(f"File not found: {self.input_file}")
            return []
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON format in file: {self.input_file}")
            return []
    
    def categorize_link(self, link: str) -> str:
        """
        Categorize a single link.

        Priority order:
        1. First check if it's a social media link
        2. Then check if it's a miscellaneous link (no bank keyword)
        3. Then check if it's a file link
        4. Finally check if it's a bank-related link

        Returns:
            Category name: 'social_media', 'misc', 'file', or 'bank'
        """
        # Check if it's a social media link
        if self.social_media_pattern.search(link):
            return 'social_media'
        
        # Check if it's NOT a bank link (miscellaneous)
        elif not self.bank_pattern.search(link):
            return 'misc'
        
        # Check if it's a file link
        elif self.file_pattern.search(link):
            return 'file'
        
        # If none of the above, it's a bank link
        else:
            return 'bank'
    
    def process_links_chunk(self, links_chunk: List[str], chunk_index: int, total_chunks: int) -> None:
        """Process a chunk of links and categorize them."""
        local_file_links: Set[str] = set()
        local_social_media_links: Set[str] = set()
        local_bank_links: Set[str] = set()
        local_misc_links: Set[str] = set()
        
        for i, link in enumerate(links_chunk):
            category = self.categorize_link(link)
            
            if category == 'file':
                local_file_links.add(link)
            elif category == 'social_media':
                local_social_media_links.add(link)
            elif category == 'bank':
                local_bank_links.add(link)
            else:
                local_misc_links.add(link)
            
            # Update progress every 100 links
            if i % 100 == 0:
                # Calculate progress: current chunk progress + completed chunks
                chunk_progress = (i / len(links_chunk)) / total_chunks
                completed_chunks_progress = chunk_index / total_chunks
                self.progress = min(99.0, (completed_chunks_progress + chunk_progress) * 100)
        
        # Acquire lock once to update all collections
        with self.lock:
            self.file_links.update(local_file_links)
            self.social_media_links.update(local_social_media_links)
            self.bank_links.update(local_bank_links)
            self.misc_links.update(local_misc_links)
        
        # Update progress for completed chunk
        self.progress = min(99.0, ((chunk_index + 1) / total_chunks) * 100)
    
    def divide_links_into_chunks(self, links: List[str]) -> List[List[str]]:
        """Divide links into equal chunks for worker threads."""
        chunk_size = max(1, len(links) // self.num_workers)
        return [
            links[i:i + chunk_size]
            for i in range(0, len(links), chunk_size)
        ]
    
    def process(self) -> Dict[str, Any]:
        """Process all links and categorize them."""
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        
        self.logger.info(f"Starting to process links from {self.input_file}")
        
        # Load links from JSON
        all_links = self.load_links()
        
        # Create empty result structure for when no links are found
        empty_results = {
            'summary': {
                'total_links': 0,
                'file_links_count': 0,
                'social_media_links_count': 0,
                'bank_links_count': 0,
                'misc_links_count': 0,
                'processing_time_seconds': 0
            },
            'file_links': [],
            'social_media_links': [],
            'bank_links': [],
            'misc_links': []
        }
        
        if not all_links:
            self.logger.error("No links found to process")
            self.status = "error"
            
            # Save empty results to JSON
            empty_results['summary']['processing_time_seconds'] = time.time() - self.start_time
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(empty_results, f, indent=2)
                
            self.logger.info(f"Empty results saved to {self.output_file}")
            return empty_results
        
        total_links = len(all_links)
        self.logger.info(f"Loaded {total_links} links for processing")
        
        # Divide links into chunks for parallel processing
        chunks = self.divide_links_into_chunks(all_links)
        total_chunks = len(chunks)
        self.logger.info(f"Divided links into {total_chunks} chunks for {self.num_workers} workers")
        
        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [
                executor.submit(self.process_links_chunk, chunk, idx, total_chunks)
                for idx, chunk in enumerate(chunks)
            ]
            
            # Wait for all tasks to complete
            for future in futures:
                future.result()
        
        # Convert sets to lists for JSON serialization
        results = {
            'summary': {
                'total_links': total_links,
                'file_links_count': len(self.file_links),
                'social_media_links_count': len(self.social_media_links),
                'bank_links_count': len(self.bank_links),
                'misc_links_count': len(self.misc_links),
                'processing_time_seconds': time.time() - self.start_time
            },
            'file_links': sorted(list(self.file_links)),
            'social_media_links': sorted(list(self.social_media_links)),
            'bank_links': sorted(list(self.bank_links)),
            'misc_links': sorted(list(self.misc_links))
        }
        
        # Save results to JSON
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        self.logger.info(f"Processing completed in {time.time() - self.start_time:.2f} seconds")
        self.logger.info(f"Results saved to {self.output_file}")
        
        # Update status
        self.status = "completed"
        self.progress = 100.0
        
        return results
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the processor."""
        return {
            'status': self.status,
            'progress': self.progress,
            'file_links_count': len(self.file_links),
            'social_media_links_count': len(self.social_media_links),
            'bank_links_count': len(self.bank_links),
            'misc_links_count': len(self.misc_links),
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0
        }
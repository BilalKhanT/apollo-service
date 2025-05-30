import json
import re
import threading
import os
import time
import logging
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Set, Any, Optional, Tuple

class LinkProcessor:
    def __init__(
        self,
        input_file: str = "all_links.json",
        output_file: str = "categorized_links.json",
        num_workers: int = 20,
        file_extensions: Optional[List[str]] = None,
        social_media_keywords: Optional[List[str]] = None,
        bank_keywords: Optional[List[str]] = None,
        chunk_size: int = 500  
    ):
        self.logger = self._setup_logger()
        self.input_file = input_file
        self.output_file = output_file
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        self.file_extensions = file_extensions or [
            'pdf', 'xls', 'xlsx', 'doc', 'docx', 'ppt', 'pptx',
            'csv', 'txt', 'rtf', 'zip', 'rar', 'tar', 'gz', 'xlsb'
        ]
        self.social_media_keywords = social_media_keywords or [
            'instagram', 'facebook', 'linkedin', 'twitter', 'tiktok',
            'youtube', 'apps.google', 'appstore', 'play.google', 'app.apple'
        ]
        self.bank_keywords = bank_keywords or ['bafl', 'falah']
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
        self.file_links: Set[str] = set()
        self.social_media_links: Set[str] = set()
        self.bank_links: Set[str] = set()
        self.misc_links: Set[str] = set()
        self.lock = threading.Lock()
        self.work_queue = queue.Queue()
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.processed_counter = 0
        self.processed_lock = threading.Lock()
        self.logger.info(f"LinkProcessor initialized with {num_workers} workers and chunk_size={chunk_size}")
    
    def _setup_logger(self):
        logger = logging.getLogger("LinkProcessor")
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def load_links(self) -> List[str]:
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
        if self.social_media_pattern.search(link):
            return 'social_media'

        elif not self.bank_pattern.search(link):
            return 'misc'

        elif self.file_pattern.search(link):
            return 'file'

        else:
            return 'bank'
    
    def process_chunk(self, chunk_id: int, links: List[str]) -> Tuple[Set[str], Set[str], Set[str], Set[str]]:
        local_file_links: Set[str] = set()
        local_social_media_links: Set[str] = set()
        local_bank_links: Set[str] = set()
        local_misc_links: Set[str] = set()
        
        self.logger.info(f"Processing chunk {chunk_id} with {len(links)} links")
        for i, link in enumerate(links):
            category = self.categorize_link(link)
            
            if category == 'file':
                local_file_links.add(link)
            elif category == 'social_media':
                local_social_media_links.add(link)
            elif category == 'bank':
                local_bank_links.add(link)
            else:
                local_misc_links.add(link)

            if i % 100 == 0:
                with self.processed_lock:
                    self.processed_counter += 100

        with self.processed_lock:
            remaining = len(links) % 100
            if remaining > 0:
                self.processed_counter += remaining
        
        return local_file_links, local_social_media_links, local_bank_links, local_misc_links
    
    def worker_thread(self, worker_id: int) -> None:
        while True:
            try:
                chunk_data = self.work_queue.get(block=False)
                if chunk_data is None:
                    self.work_queue.task_done()
                    break
                
                chunk_id, links = chunk_data

                local_results = self.process_chunk(chunk_id, links)

                with self.lock:
                    self.file_links.update(local_results[0])
                    self.social_media_links.update(local_results[1])
                    self.bank_links.update(local_results[2])
                    self.misc_links.update(local_results[3])

                self.work_queue.task_done()

                total_links = len(self.file_links) + len(self.social_media_links) + len(self.bank_links) + len(self.misc_links)
                self.logger.debug(f"Worker {worker_id} completed chunk {chunk_id}. Total links processed: {total_links}")
                
            except queue.Empty:
                break
            except Exception as e:
                self.logger.error(f"Error in worker {worker_id}: {str(e)}")
                self.work_queue.task_done()
    
    def process(self) -> Dict[str, Any]:
        import threading
        
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        self.processed_counter = 0
        
        self.logger.info(f"Starting to process links from {self.input_file}")
        all_links = self.load_links()
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
            empty_results['summary']['processing_time_seconds'] = time.time() - self.start_time
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(empty_results, f, indent=2)
                
            self.logger.info(f"Empty results saved to {self.output_file}")
            return empty_results
        
        total_links = len(all_links)
        self.logger.info(f"Loaded {total_links} links for processing")
        self.file_links.clear()
        self.social_media_links.clear()
        self.bank_links.clear()
        self.misc_links.clear()

        chunks = []
        for i in range(0, total_links, self.chunk_size):
            chunk_id = i // self.chunk_size
            chunk_links = all_links[i:i + self.chunk_size]
            chunks.append((chunk_id, chunk_links))
        
        self.logger.info(f"Created {len(chunks)} chunks of size {self.chunk_size}")
        for chunk in chunks:
            self.work_queue.put(chunk)

        workers = []
        for i in range(self.num_workers):
            worker = threading.Thread(target=self.worker_thread, args=(i,))
            worker.daemon = True
            worker.start()
            workers.append(worker)

        def update_progress():
            while sum(worker.is_alive() for worker in workers) > 0:
                if total_links > 0:
                    with self.processed_lock:
                        processed = min(self.processed_counter, total_links)
                        self.progress = min(99.0, (processed / total_links) * 100)
                
                self.logger.info(f"Progress: {self.progress:.1f}% ({processed}/{total_links} links processed)")
                time.sleep(1.0)  
        
        progress_thread = threading.Thread(target=update_progress)
        progress_thread.daemon = True
        progress_thread.start()

        self.work_queue.join()

        for _ in range(self.num_workers):
            self.work_queue.put(None)

        for worker in workers:
            worker.join()

        self.progress = 99.9 
        progress_thread.join(timeout=0.5)  

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

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        self.logger.info(f"Processing completed in {time.time() - self.start_time:.2f} seconds")
        self.logger.info(f"Results saved to {self.output_file}")
        self.status = "completed"
        self.progress = 100.0
        return results
    
    def get_status(self) -> Dict[str, Any]:
        return {
            'status': self.status,
            'progress': self.progress,
            'file_links_count': len(self.file_links),
            'social_media_links_count': len(self.social_media_links),
            'bank_links_count': len(self.bank_links),
            'misc_links_count': len(self.misc_links),
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0
        }
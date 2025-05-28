import os
import time
import requests
import re
import threading
import logging
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Callable

class FacebookScrapingService:
    
    def __init__(
        self,
        access_token: str,
        page_id: str,
        output_dir: str = "facebook_data",
        max_workers: int = 20,
        progress_update_interval: int = 5,
        batch_size: int = 50
    ):
        self.logger = self._setup_logger()

        self.access_token = access_token
        self.page_id = page_id
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.progress_update_interval = progress_update_interval
        self.batch_size = batch_size
        self.base_url = "https://graph.facebook.com"
        self.api_version = "v18.0"
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.posts_processed = 0
        self.posts_found = 0
        self.posts_failed = 0
        self.current_keyword = ""
        self.current_batch = 0
        self.total_batches = 0
        self.error = None
        self.all_posts = []
        self.processed_posts_data = []  
        self.category_counts = {}
        self.keyword_matches = {}
        self.counter_lock = threading.Lock()
        self.results_lock = threading.Lock()
        self.posts_data_lock = threading.Lock()
        self.stop_event = threading.Event()  
        self.task_id = None
        self.task_manager = None
        self.progress_callback = None
        
        self.logger.info(f"FacebookScrapingService initialized with output_dir={output_dir}, max_workers={max_workers}, batch_size={batch_size}")

    def _setup_logger(self):
        logger = logging.getLogger("FacebookScrapingService")
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
        if self.posts_found > 0:
            base_progress = (self.posts_processed / self.posts_found) * 85.0
            self.progress = 5.0 + min(85.0, base_progress)
        else:
            self.progress = 5.0

        should_update = (
            force or 
            (self.posts_processed % self.progress_update_interval == 0) or
            (self.current_batch % max(1, self.progress_update_interval // 5) == 0)
        )
        
        if not should_update:
            return

        progress_info = {
            "status": self.status,
            "progress": self.progress,
            "posts_processed": self.posts_processed,
            "posts_found": self.posts_found,
            "posts_failed": self.posts_failed,
            "current_keyword": self.current_keyword,
            "current_batch": self.current_batch,
            "total_batches": self.total_batches,
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
                        "facebook_scrape_partial_results": {
                            "posts_processed": self.posts_processed,
                            "posts_found": self.posts_found,
                            "posts_failed": self.posts_failed,
                            "current_keyword": self.current_keyword,
                            "current_batch": self.current_batch
                        }
                    }
                )
                
                if (self.posts_processed % (self.progress_update_interval * 2) == 0 or force) and self.task_manager:
                    task_manager.publish_log(
                        self.task_id,
                        f"Facebook scraping progress: {self.posts_processed}/{self.posts_found} posts processed, "
                        f"{self.posts_failed} failed, batch {self.current_batch}/{self.total_batches}, "
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

        if self.posts_processed % (self.progress_update_interval * 2) == 0 or force:
            self.logger.info(
                f"Progress: {self.posts_processed}/{self.posts_found} posts processed, "
                f"{self.posts_failed} failed, batch {self.current_batch}/{self.total_batches}, "
                f"{self.progress:.1f}%"
            )

    def sanitize_filename(self, filename: str) -> str:
        invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename

    def has_loose_match(self, text: str, keywords: List[str]) -> bool:
        if not text or not keywords:
            return False
        
        text_lower = text.lower()
        for keyword in keywords:
            keyword_lower = keyword.lower()
            if keyword_lower in text_lower:
                return True
        return False

    def has_word_match(self, text: str, keywords: List[str]) -> bool:
        if not text or not keywords:
            return False
        
        text_lower = text.lower()
        for keyword in keywords:
            keyword_lower = keyword.lower()
            pattern = r'\b' + re.escape(keyword_lower) + r'\b'
            if re.search(pattern, text_lower):
                return True
        return False

    def count_word_matches(self, text: str, keywords: List[str]) -> int:
        if not text or not keywords:
            return 0
        
        text_lower = text.lower()
        match_count = 0
        for keyword in keywords:
            keyword_lower = keyword.lower()
            pattern = r'\b' + re.escape(keyword_lower) + r'\b'
            if re.search(pattern, text_lower):
                match_count += 1
        return match_count

    def categorize_post_content(self, text: str) -> str:
        if not text:
            return "uncategorized"
        
        categories = {
            "mobilePhones": ["iphone", "samsung", "oppo", "vivo", "xiaomi", "infinix", "tecno", "huawei", "oneplus", "realme", "honor", "nokia", "motorola", "google pixel", "galaxy", "reno", "redmi", "poco", "spark", "camon", "battery", "A34", "A15", "A16", "S25", "Ultra", "Pro Max", "smartphone", "PTA", "128GB", "256GB", "Pro"],
            "vehicles": ["bike", "motor", "BMW", "R nineT", "K1600", "Bagger", "Scrambler", "DML 3100", "ride", "motorcycle", "car", "vehicle", "automotive"],
            "electronics": ["electronics", "appliance", "gadget", "tech", "USB-C Power Adapter", "home", "device", "chromebook", "laptop", "macbook","AC"],
            "foodDining": ["dining", "restaurant", "food", "meal", "Foodpanda", "Coffee", "Fresh Basket", "Exclusive by J", "cafe", "grill", "Monarca", "SPAR", "Greeno", "refreshment", "grocery", "juice", "thirst", "hunger", "Pizza","meals"],
            "healthWellness": ["health", "OlaDoc", "telehealth", "consultation", "OPD", "surgeries", "medical", "wellness"],
            "lifestyle": ["fragrance", "outfit", "shopping", "Ramadan Scentsation", "aroma", "fashion", "discount voucher", "style", "daraz"],
            "seasonal": ["Ramadan", "Qurbani", "Eid", "festival", "seasonal", "Ramzan", "Spring Time"],
            "premiumBanking": ["Silver", "Gold", "priority", "luxury", "World", "Platinum"],
            "business": ["business", "corporate", "Rehmat Business", "karobar", "entrepreneur"],
            "socialResponsibility": ["Qabil", "disability", "differently-abled", "inclusion", "PWD", "Persons with Disabilities", "education", "marriage"],
            "security": ["security", "safety", "protect", "vigilant", "fraud", "Ponzi", "Pyramid schemes"],
        }

        category_matches = {}
        for category, keywords in categories.items():
            matches = self.count_word_matches(text, keywords)
            if matches > 0:
                category_matches[category] = matches
        
        if category_matches:
            best_category = max(category_matches.items(), key=lambda x: x[1])[0]
            return best_category
        else:
            return "other"

    def get_all_posts(self, start_date: str, end_date: str, keywords: List[str]) -> List[Dict[str, Any]]:
        base_url = f"{self.base_url}/{self.api_version}/{self.page_id}/feed"
        
        start_timestamp = int(time.mktime(datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
        end_timestamp = int(time.mktime(datetime.strptime(end_date, "%Y-%m-%d").timetuple()))
        
        params = {
            'access_token': self.access_token,
            'fields': 'id,message,created_time,attachments',
            'since': start_timestamp,
            'until': end_timestamp,
        }
        
        all_posts = []
        filtered_posts = []
        next_url = base_url
        api_call_count = 0
        max_api_calls = 100  
        
        while next_url and not self.stop_event.is_set() and api_call_count < max_api_calls:
            if self.stop_event.is_set():
                self.logger.info(f"Stop event detected during API calls after {api_call_count} calls")
                break
                
            try:
                api_call_count += 1

                response = requests.get(next_url, params=params, timeout=10)

                if self.stop_event.is_set():
                    self.logger.info(f"Stop event detected after API call {api_call_count}")
                    break
                    
                response.raise_for_status()
                data = response.json()
                
                if 'data' in data:
                    all_posts.extend(data['data'])

                    if self.stop_event.is_set():
                        self.logger.info(f"Stop event detected after processing batch {api_call_count}")
                        break
                    
                    if 'paging' in data and 'next' in data['paging']:
                        next_url = data['paging']['next']
                        params = {}  
                    else:
                        next_url = None
                else:
                    error_msg = data.get('error', {}).get('message', 'Unknown error')

                    if self.stop_event.is_set():
                        self.logger.info("API error after stop event was set, likely due to stopping")
                        break
                    else:
                        raise Exception(f"Facebook API error: {error_msg}")

                time.sleep(0.05)  

                if self.stop_event.is_set():
                    self.logger.info(f"Stop event detected after sleep, ending API calls")
                    break
                    
            except requests.exceptions.RequestException as e:
                if self.stop_event.is_set():
                    self.logger.info(f"Request exception after stop event: {str(e)}")
                    break
                else:
                    raise Exception(f"Facebook API request failed: {str(e)}")

        if self.stop_event.is_set():
            self.logger.info(f"Stopping API calls early due to stop event. Retrieved {len(all_posts)} posts so far.")
            return []  

        for post in all_posts:
            if self.stop_event.is_set():
                self.logger.info("Stop event detected during post filtering")
                break
                
            message = post.get('message', '')
            if self.has_loose_match(message, keywords):
                filtered_posts.append(post)
        
        return filtered_posts

    def process_post_batch(self, batch_id: int, posts_batch: List[Dict[str, Any]], 
                          keywords: List[str], keyword_folders: Dict[str, str], 
                          final_output_dir: str) -> Dict[str, Any]:
        batch_result = {
            "batch_id": batch_id,
            "posts_processed": 0,
            "posts_failed": 0,
            "category_counts": {},
            "keyword_matches": {keyword: {"loose_matches": 0, "strict_matches": 0} for keyword in keywords},
            "processed_posts": []
        }
        
        try:
            self.logger.debug(f"Processing batch {batch_id} with {len(posts_batch)} posts")
            
            for post in posts_batch:
                if self.stop_event.is_set():
                    self.logger.info(f"Stop event detected in batch {batch_id}, stopping post processing")
                    break
                    
                try:
                    message = post.get("message", "")
                    post_id = post.get("id", "unknown")
                    created_time = post.get("created_time", "")
                    attachments = post.get("attachments", {})
                    save_folders = []
                    matching_keywords = []
                    for keyword in keywords:
                        if self.has_loose_match(message, [keyword]):
                            save_folders.append(keyword_folders[keyword.lower()])
                            matching_keywords.append(keyword)
                            batch_result["keyword_matches"][keyword]["loose_matches"] += 1

                            if self.has_word_match(message, [keyword]):
                                batch_result["keyword_matches"][keyword]["strict_matches"] += 1

                    if not save_folders:
                        save_folders = [final_output_dir]

                    offer_category = self.categorize_post_content(message)
                    batch_result["category_counts"][offer_category] = batch_result["category_counts"].get(offer_category, 0) + 1

                    post_data = {
                        "id": post_id,
                        "message": message,
                        "created_time": created_time,
                        "category": offer_category,
                        "attachments": self._process_attachments(attachments),
                        "matching_keywords": matching_keywords
                    }
                    batch_result["processed_posts"].append(post_data)

                    for folder in save_folders:
                        try:
                            folder_name = os.path.basename(folder)
                            created_time_safe = created_time.replace(":", "-") if created_time else "unknown"
                            
                            file_path = os.path.join(folder, f"fb_post_{folder_name}_{offer_category}_{created_time_safe}.md")

                            os.makedirs(folder, exist_ok=True)
                            
                            with open(file_path, "w", encoding="utf-8") as f:
                                f.write(f"# Facebook Post - {created_time}\n\n")
                                
                                if message:
                                    f.write(message + "\n\n")

                                f.write(f"**Post ID:** {post_id}\n")
                                f.write(f"**Created:** {created_time}\n")
                                f.write(f"**Category:** {offer_category}\n")
                                f.write(f"**Matching Keywords:** {', '.join(matching_keywords)}\n\n")

                                if "attachments" in post and "data" in post["attachments"]:
                                    f.write("## Attachments\n\n")
                                    for attachment in post["attachments"]["data"]:
                                        if "media" in attachment and "image" in attachment["media"]:
                                            image_url = attachment["media"]["image"].get("src", "")
                                            f.write(f"![Image]({image_url})\n\n")
                        except Exception as file_error:
                            self.logger.warning(f"Error saving post to file {file_path}: {str(file_error)}")
                    
                    batch_result["posts_processed"] += 1
                    
                except Exception as e:
                    self.logger.warning(f"Error processing post in batch {batch_id}: {str(e)}")
                    batch_result["posts_failed"] += 1
            
            return batch_result
            
        except Exception as e:
            self.logger.error(f"Error in batch {batch_id}: {str(e)}")
            batch_result["posts_failed"] = len(posts_batch)
            return batch_result

    def _process_attachments(self, attachments: Dict[str, Any]) -> List[Dict[str, Any]]:
        processed_attachments = []
        
        try:
            if not attachments or "data" not in attachments:
                return processed_attachments
            
            for attachment in attachments["data"]:
                attachment_data = {}

                if "media" in attachment:
                    media = attachment["media"]
                    if "image" in media:
                        attachment_data["type"] = "image"
                        attachment_data["url"] = media["image"].get("src", "")
                        attachment_data["width"] = media["image"].get("width")
                        attachment_data["height"] = media["image"].get("height")

                attachment_data["title"] = attachment.get("title", "")
                attachment_data["description"] = attachment.get("description", "")
                attachment_data["url"] = attachment.get("url", "")
                
                if attachment_data:
                    processed_attachments.append(attachment_data)
                    
        except Exception as e:
            self.logger.warning(f"Error processing attachments: {str(e)}")
        
        return processed_attachments

    def update_results(self, batch_result: Dict[str, Any]) -> None:
        with self.results_lock:
            for category, count in batch_result["category_counts"].items():
                self.category_counts[category] = self.category_counts.get(category, 0) + count

            for keyword, matches in batch_result["keyword_matches"].items():
                if keyword not in self.keyword_matches:
                    self.keyword_matches[keyword] = {"loose_matches": 0, "strict_matches": 0}
                self.keyword_matches[keyword]["loose_matches"] += matches["loose_matches"]
                self.keyword_matches[keyword]["strict_matches"] += matches["strict_matches"]
        
        if "processed_posts" in batch_result:
            with self.posts_data_lock:
                self.processed_posts_data.extend(batch_result["processed_posts"])

        with self.counter_lock:
            self.posts_processed += batch_result["posts_processed"]
            self.posts_failed += batch_result["posts_failed"]
            self.current_batch += 1
            self.publish_progress()

    def scrape_facebook_posts(
        self,
        keywords: List[str],
        days: int,
        task_id: Optional[str] = None,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Dict[str, Any]:

        if task_id:
            self.set_task_id(task_id)
        
        if callback:
            self.set_progress_callback(callback)

        self.start_time = time.time()
        self.status = "initializing"
        self.progress = 5.0
        self.posts_processed = 0
        self.posts_found = 0
        self.posts_failed = 0
        self.current_keyword = ""
        self.current_batch = 0
        self.total_batches = 0
        self.error = None
        self.all_posts = []
        self.processed_posts_data = []
        self.category_counts = {}
        self.keyword_matches = {}
        
        self.publish_progress(force=True)
        
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
            keywords_str = "_".join(keywords) if keywords else "all"

            final_output_dir = f"{self.output_dir}/facebook_data_{timestamp}_{keywords_str}"
            os.makedirs(final_output_dir, exist_ok=True)
            
            self.logger.info(f"Starting Facebook scraping to directory: {final_output_dir}")

            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")

            self.status = "fetching_posts"
            self.publish_progress(force=True)

            if self.stop_event.is_set():
                self.logger.info("Stop event detected before API call")
                self.status = "stopped"
                self.progress = 95.0
                self.publish_progress(force=True)
                return {
                    "status": "stopped",
                    "posts_processed": 0,
                    "posts_found": 0,
                    "posts_failed": 0,
                    "execution_time_seconds": time.time() - self.start_time,
                    "posts_data": []
                }

            self.logger.info(f"Fetching posts from {start_date} to {end_date}")
            posts = self.get_all_posts(start_date, end_date, keywords)

            if self.stop_event.is_set():
                self.logger.info("Stop event detected after API call")
                self.status = "stopped"
                self.progress = 95.0
                self.publish_progress(force=True)
                return {
                    "status": "stopped",
                    "posts_processed": 0,
                    "posts_found": len(posts) if posts else 0,
                    "posts_failed": 0,
                    "execution_time_seconds": time.time() - self.start_time,
                    "posts_data": []
                }
            
            self.posts_found = len(posts)
            self.all_posts = posts
            
            self.logger.info(f"Retrieved {len(posts)} posts with loose keyword matching")

            if self.stop_event.is_set():
                self.logger.info("Stop event detected before processing posts")
                self.status = "stopped"
                self.progress = 95.0
                self.publish_progress(force=True)
                return {
                    "status": "stopped",
                    "posts_processed": 0,
                    "posts_found": self.posts_found,
                    "posts_failed": 0,
                    "execution_time_seconds": time.time() - self.start_time,
                    "posts_data": []
                }
            
            if self.posts_found == 0:
                self.logger.warning("No posts found matching the criteria")
                self.status = "completed"
                self.progress = 100.0
                self.publish_progress(force=True)
                
                return {
                    "status": "completed",
                    "posts_processed": 0,
                    "posts_found": 0,
                    "posts_failed": 0,
                    "categories_found": {},
                    "keyword_matches": {},
                    "output_directory": final_output_dir,
                    "execution_time_seconds": time.time() - self.start_time,
                    "date_range": {"start_date": start_date, "end_date": end_date},
                    "posts_data": []
                }

            self.status = "processing_posts"
            self.publish_progress(force=True)

            keyword_folders = {}
            for keyword in keywords:
                keyword_folder = os.path.join(final_output_dir, self.sanitize_filename(keyword))
                os.makedirs(keyword_folder, exist_ok=True)
                keyword_folders[keyword.lower()] = keyword_folder

            for keyword in keywords:
                self.keyword_matches[keyword] = {"loose_matches": 0, "strict_matches": 0}

            batches = []
            for i in range(0, len(posts), self.batch_size):
                batch_id = i // self.batch_size
                batch_posts = posts[i:i + self.batch_size]
                batches.append((batch_id, batch_posts))
            
            self.total_batches = len(batches)
            self.logger.info(f"Processing {self.posts_found} posts in {self.total_batches} batches using {self.max_workers} workers")

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(
                        self.process_post_batch, 
                        batch_id, 
                        batch_posts, 
                        keywords, 
                        keyword_folders, 
                        final_output_dir
                    ): (batch_id, batch_posts)
                    for batch_id, batch_posts in batches
                }

                for future in as_completed(futures):
                    batch_id, batch_posts = futures[future]
                    
                    if self.stop_event.is_set():
                        self.logger.info("Facebook scraping stopped by user request")
                        break
                    
                    try:
                        batch_result = future.result()

                        self.update_results(batch_result)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing batch {batch_id}: {str(e)}")
                        self.logger.error(traceback.format_exc())
                        with self.counter_lock:
                            self.posts_failed += len(batch_posts)
                            self.current_batch += 1
                            self.publish_progress()

            if not self.stop_event.is_set():
                self._create_summary_file(final_output_dir, keywords, start_date, end_date)

            if self.stop_event.is_set():
                self.status = "stopped"
                self.progress = 95.0
                self.logger.info("Facebook scraping process was stopped by user")
            else:
                self.status = "completed"
                self.progress = 100.0
                self.logger.info("Facebook scraping process completed successfully")
            
            self.publish_progress(force=True)

            execution_time = time.time() - self.start_time
            
            results = {
                "status": self.status,  
                "posts_processed": self.posts_processed,
                "posts_found": self.posts_found,
                "posts_failed": self.posts_failed,
                "categories_found": self.category_counts,
                "keyword_matches": self.keyword_matches,
                "output_directory": final_output_dir,
                "execution_time_seconds": execution_time,
                "date_range": {"start_date": start_date, "end_date": end_date},
                "batches_processed": self.current_batch,
                "total_batches": self.total_batches,
                "error": self.error,
                "posts_data": self.processed_posts_data if self.status == "completed" else []  
            }
            
            self.logger.info(
                f"Facebook scraping {self.status}. Processed {self.posts_processed} posts "
                f"({self.posts_failed} failed) in {self.current_batch} batches "
                f"using {self.max_workers} workers in {execution_time:.2f} seconds. "
                f"Collected {len(self.processed_posts_data)} posts for database."
            )
            
            return results
            
        except Exception as e:
            error_msg = f"Error in Facebook scraping workflow: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            
            self.status = "failed"
            self.error = error_msg
            self.publish_progress(force=True)
            
            return {
                "status": "failed",
                "error": error_msg,
                "posts_processed": self.posts_processed,
                "posts_found": self.posts_found,
                "posts_failed": self.posts_failed,
                "execution_time_seconds": time.time() - self.start_time,
                "posts_data": []  
            }

    def _create_summary_file(self, output_dir: str, keywords: List[str], start_date: str, end_date: str):
        try:
            summary_path = os.path.join(output_dir, "summary.md")
            
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write(f"# Facebook Scraping Summary\n\n")
                f.write(f"**Date Range:** {start_date} to {end_date}\n")
                f.write(f"**Keywords:** {', '.join(keywords)}\n")
                f.write(f"**Total Posts Found:** {self.posts_found}\n")
                f.write(f"**Total Posts Processed:** {self.posts_processed}\n")
                f.write(f"**Total Posts Failed:** {self.posts_failed}\n")
                f.write(f"**Posts Collected for Database:** {len(self.processed_posts_data)}\n")
                f.write(f"**Processing Method:** Parallel processing with {self.max_workers} workers\n")
                f.write(f"**Batches Processed:** {self.current_batch}/{self.total_batches}\n")
                f.write(f"**Execution Time:** {time.time() - self.start_time:.2f} seconds\n\n")
                
                if keywords:
                    f.write("## Keyword Folder Assignment\n\n")
                    for keyword in keywords:
                        loose_count = self.keyword_matches.get(keyword, {}).get("loose_matches", 0)
                        strict_count = self.keyword_matches.get(keyword, {}).get("strict_matches", 0)
                        f.write(f"- **{keyword}:**\n")
                        f.write(f"  - Posts in {keyword} folder: {loose_count} posts\n")
                        f.write(f"  - Posts with exact word '{keyword}': {strict_count} posts\n")
                
                if self.category_counts:
                    f.write("\n## Posts by Category (using strict word matching)\n\n")
                    for category, count in sorted(self.category_counts.items()):
                        f.write(f"- **{category}:** {count} posts\n")
            
            self.logger.info(f"Created summary file: {summary_path}")
            
        except Exception as e:
            self.logger.error(f"Error creating summary file: {str(e)}")

    def stop(self) -> bool:
        self.logger.info("Stopping Facebook scraping process...")
        self.stop_event.set()
        time.sleep(1)
        self.status = "stopped"
        self.progress = 95.0
        
        return True

    def get_status(self) -> Dict[str, Any]:
        return {
            'status': self.status,
            'progress': self.progress,
            'posts_processed': self.posts_processed,
            'posts_found': self.posts_found,
            'posts_failed': self.posts_failed,
            'current_keyword': self.current_keyword,
            'current_batch': self.current_batch,
            'total_batches': self.total_batches,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0,
            'error': self.error,
            'posts_data_collected': len(self.processed_posts_data) if hasattr(self, 'processed_posts_data') else 0
        }
import requests
import json
import time
import os
import threading
import logging
import traceback
from typing import Dict, List, Any, Optional, Callable

class DealScrapperService:

    BASE_HEADERS = {
        'Ownerkey': '109fcf39f8ada91483dcced4632b0d42'
    }

    CITIES_URL = 'https://secure-sdk.peekaboo.guru/klaoshcjanaij2ktnbjkmiasvtafoabxtenstn5'
    RESTAURANTS_URL = 'https://secure-sdk.peekaboo.guru/uljin2s3nitoi89njkhklgkj5'
    DEALS_URL = 'https://secure-sdk.peekaboo.guru/ksbolruuahrndcjchshjhejgjhasdo787kjieo767kjsgeskoyfgwwhkl6'
    
    def __init__(
        self,
        country: str = "Pakistan",
        language: str = "en",
        output_dir: str = "Deals and Discounts",
        max_workers: int = 20,
        request_delay: float = 0.5,
        progress_update_interval: int = 5
    ):

        self.logger = self._setup_logger()

        self.country = country
        self.language = language
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.request_delay = request_delay
        self.progress_update_interval = progress_update_interval
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.cities_processed = 0
        self.restaurants_processed = 0
        self.deals_found = 0
        self.total_cities = 0
        self.total_restaurants = 0
        self.current_city = ""
        self.current_restaurant = ""
        self.error = None
        self.cities = []
        self.restaurants_by_city = {}
        self.deals_by_restaurant = {}
        self.counter_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.task_id = None
        self.task_manager = None
        self.progress_callback = None
        self._create_directory_structure()
        
        self.logger.info(f"DealScrapperService initialized with output_dir={output_dir}, max_workers={max_workers}")
    
    def _create_directory_structure(self):
        try:
            os.makedirs(self.output_dir, exist_ok=True)

            subdirs = [
                "markdown_files",
                "json_data", 
                "city_summaries"
            ]
            
            for subdir in subdirs:
                os.makedirs(os.path.join(self.output_dir, subdir), exist_ok=True)
            
            self.logger.info(f"Created directory structure in {self.output_dir}")
            
        except Exception as e:
            self.logger.error(f"Error creating directory structure: {str(e)}")
    
    def _setup_logger(self):
        logger = logging.getLogger("DealScrapperService")
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
        if self.total_cities > 0:
            base_progress = (self.cities_processed / self.total_cities) * 85.0

            if self.cities_processed < self.total_cities:
                estimated_restaurants_per_city = 12
                if self.cities_processed > 0:
                    estimated_restaurants_per_city = max(1, self.restaurants_processed // max(1, self.cities_processed))

                current_city_restaurants = self.restaurants_processed - (self.cities_processed * estimated_restaurants_per_city)
                if current_city_restaurants > 0:
                    partial_city_progress = min(1.0, current_city_restaurants / estimated_restaurants_per_city)
                    partial_progress = (partial_city_progress / self.total_cities) * 85.0
                    base_progress += partial_progress
            
            self.progress = 5.0 + min(85.0, base_progress)

        should_update = (
            force or 
            (self.cities_processed % max(1, self.progress_update_interval // 5) == 0) or
            (self.restaurants_processed % self.progress_update_interval == 0)
        )
        
        if not should_update:
            return

        progress_info = {
            "status": self.status,
            "progress": self.progress,
            "cities_processed": self.cities_processed,
            "restaurants_processed": self.restaurants_processed,
            "deals_found": self.deals_found,
            "total_cities": self.total_cities,
            "total_restaurants": self.total_restaurants,
            "current_city": self.current_city,
            "current_restaurant": self.current_restaurant,
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
                        "deal_scrape_partial_results": {
                            "cities_processed": self.cities_processed,
                            "restaurants_processed": self.restaurants_processed,
                            "deals_found": self.deals_found,
                            "total_cities": self.total_cities,
                            "current_city": self.current_city
                        }
                    }
                )

                if (self.restaurants_processed % (self.progress_update_interval * 2) == 0 or force) and self.task_manager:
                    task_manager.publish_log(
                        self.task_id,
                        f"Deal scraping progress: {self.cities_processed}/{self.total_cities} cities processed, "
                        f"{self.restaurants_processed} restaurants, {self.deals_found} deals found, "
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

        if self.restaurants_processed % (self.progress_update_interval * 2) == 0 or force:
            self.logger.info(
                f"Progress: {self.cities_processed}/{self.total_cities} cities processed, "
                f"{self.restaurants_processed} restaurants, {self.deals_found} deals found, "
                f"{self.progress:.1f}%"
            )
    
    def sanitize_filename(self, name: str) -> str:
        return name.replace('/', '-').replace('\\', '-').replace(' ', '-').replace(':', '-').replace('?', '')
    
    def fetch_cities(self) -> List[str]:
        if self.stop_event.is_set():
            return []
            
        self.logger.info("Fetching available cities...")

        data = {
            "mstoaw": self.language,
            "mnakls": "300",
            "opmsta": "0",
            "n4ja3s": self.country,
            "klaosw": False
        }

        try:
            response = requests.post(self.CITIES_URL, headers=self.BASE_HEADERS, json=data)

            if response.status_code == 200:
                cities = [item['city'] for item in response.json()]
                self.logger.info(f"Found {len(cities)} cities")
                return cities
            else:
                error_msg = f"Error fetching cities: {response.status_code}"
                self.logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            error_msg = f"Exception while fetching cities: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    def fetch_restaurants(self, city: str) -> List[Dict[str, Any]]:
        if self.stop_event.is_set():
            return []
            
        self.logger.debug(f"Fetching restaurants for {city}...")

        data = {
            "fksyd": city,
            "n4ja3s": self.country,
            "js6nwf": "0",
            "pan3ba": "0",
            "mstoaw": self.language,
            "angaks": "all",
            "j87asn": "_all",
            "makthya": "trending",
            "mnakls": 12,
            "opmsta": "0",
            "kaiwnua": "_all",
            "klaosw": False
        }

        try:
            response = requests.post(self.RESTAURANTS_URL, headers=self.BASE_HEADERS, json=data)

            if response.status_code == 200:
                restaurant_data = response.json()
                city_restaurants = []

                for restaurant in restaurant_data:
                    restaurant_info = {
                        "entityId": restaurant.get('entityId'),
                        "name": restaurant.get('name'),
                        "package": restaurant.get('package'),
                        "description": restaurant.get('description'),
                        "contactNumber": restaurant.get('contactNumber'),
                        "rating": restaurant.get('entityRating'),
                        "website": restaurant.get('website'),
                        "facebook": restaurant.get('facebook'),
                        "instagram": restaurant.get('instagram'),
                        "email": restaurant.get('email'),
                        "totalAssocationDeals": restaurant.get('totalAssocationDeals'),
                        "totalReviews": restaurant.get('totalReviews')
                    }
                    city_restaurants.append(restaurant_info)

                self.logger.debug(f"Found {len(city_restaurants)} restaurants in {city}")
                return city_restaurants
            else:
                error_msg = f"Error fetching restaurants for {city}: {response.status_code}"
                self.logger.error(error_msg)
                return []

        except Exception as e:
            error_msg = f"Exception while fetching restaurants for {city}: {str(e)}"
            self.logger.error(error_msg)
            return []
    
    def fetch_deals(self, city: str, restaurant: Dict[str, Any]) -> List[Dict[str, Any]]:
        if self.stop_event.is_set():
            return []
            
        entity_id = restaurant.get('entityId')
        restaurant_name = restaurant.get('name')

        self.logger.debug(f"  Fetching deals for: {restaurant_name}")

        deals_data = {
            "fksyd": city,
            "n4ja3s": self.country,
            "js6nwf": "0",
            "pan3ba": "0",
            "mstoaw": self.language,
            "cotuia": entity_id,
            "nai3asnu": "All",
            "ia3uas": "All",
            "kaiwnua": "_all",
            "matsw": restaurant_name,
            "yudwq": "_all",
            "njsue": "sdk",
            "hgoeni": entity_id,
            "mnakls": "100",
            "opmsta": "0",
            "mghes": "true",
            "klaosw": False,
            "makthya": "discount"
        }

        try:
            response = requests.post(self.DEALS_URL, headers=self.BASE_HEADERS, json=deals_data)

            if response.status_code != 200:
                self.logger.warning(f"    Error: {restaurant_name} - {response.status_code}")
                return []

            deals = response.json()
            results = []

            for deal in deals:
                association_names = [
                    assoc.get('name')
                    for assoc in deal.get('associations', [])
                    if isinstance(assoc, dict) and 'name' in assoc
                ]

                results.append({
                    "dealId": deal.get('dealId'),
                    "title": deal.get('title'),
                    "startDate": deal.get('startDate'),
                    "endDate": deal.get('endDate'),
                    "poweredBy": deal.get('poweredBy'),
                    "percentageValue": deal.get('percentageValue'),
                    "description": deal.get('description'),
                    "targetBranches": deal.get('targetBranches', []),
                    "associations": association_names,
                    "isRedeemable": deal.get('isRedeemable')
                })

            self.logger.debug(f"    Found {len(results)} deals for {restaurant_name}")
            return results

        except Exception as e:
            self.logger.warning(f"    Exception: {restaurant_name} - {str(e)}")
            return []
    
    def write_city_markdown(self, city: str, restaurants_deals: Dict[str, List[Dict[str, Any]]]) -> None:
        markdown_dir = os.path.join(self.output_dir, "markdown_files")
        file_path = os.path.join(markdown_dir, f"{self.sanitize_filename(city)}_deals.md")

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"# Restaurant Deals in {city}\n\n")
            f.write(f"*Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')} (PKT)*\n\n")

            restaurants_with_deals = {k: v for k, v in restaurants_deals.items() if v}
            
            if not restaurants_with_deals:
                f.write("No restaurant deals found for this city.\n")
                return

            total_restaurants = len(restaurants_with_deals)
            total_deals = sum(len(deals) for deals in restaurants_with_deals.values())
            f.write(f"**Summary:** {total_deals} deals found across {total_restaurants} restaurants\n\n")

            f.write("## Table of Contents\n\n")
            for restaurant_name in restaurants_with_deals.keys():
                anchor = restaurant_name.lower().replace(' ', '-').replace('&', 'and')
                f.write(f"- [{restaurant_name}](#{anchor}) ({len(restaurants_with_deals[restaurant_name])} deals)\n")
            f.write("\n---\n\n")

            for restaurant_name, deals in restaurants_with_deals.items():
                f.write(f"## {restaurant_name}\n\n")

                for i, deal in enumerate(deals, 1):
                    f.write(f"### Deal {i}: {deal['title']}\n")
                    f.write(f"- **Deal ID:** {deal['dealId']}\n")
                    f.write(f"- **Start Date:** {deal['startDate']}\n")
                    f.write(f"- **End Date:** {deal['endDate']}\n")
                    f.write(f"- **Powered By:** {deal['poweredBy']}\n")
                    if deal['percentageValue']:
                        f.write(f"- **Discount:** {deal['percentageValue']}%\n")
                    f.write(f"- **Description:** {deal['description']}\n")
                    f.write(f"- **Redeemable:** {'Yes' if deal['isRedeemable'] else 'No'}\n")
                    if deal['targetBranches']:
                        f.write(f"- **Target Branches:** {', '.join(deal['targetBranches'])}\n")
                    if deal['associations']:
                        f.write(f"- **Associations:** {', '.join(deal['associations'])}\n")
                    f.write("\n---\n\n")
        
        self.logger.debug(f"Saved deals for {city} to {file_path}")
    
    def write_city_json(self, city: str, restaurants_deals: Dict[str, List[Dict[str, Any]]]) -> None:
        json_dir = os.path.join(self.output_dir, "json_data")
        file_path = os.path.join(json_dir, f"{self.sanitize_filename(city)}_data.json")
        
        city_data = {
            "city": city,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "total_restaurants": len(restaurants_deals),
            "total_deals": sum(len(deals) for deals in restaurants_deals.values()),
            "restaurants": restaurants_deals
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(city_data, f, indent=2, ensure_ascii=False)
        
        self.logger.debug(f"Saved JSON data for {city} to {file_path}")
    
    def process_city(self, city: str) -> Dict[str, Any]:
        result = {
            "city": city,
            "restaurants_processed": 0,
            "deals_found": 0,
            "success": True,
            "error": None
        }
        
        try:
            if self.stop_event.is_set():
                result["success"] = False
                result["error"] = "Task was stopped"
                return result

            with self.counter_lock:
                self.current_city = city
                self.publish_progress()

            restaurants = self.fetch_restaurants(city)
            self.restaurants_by_city[city] = restaurants
            
            if not restaurants:
                self.logger.warning(f"No restaurants found for {city}")
                with self.counter_lock:
                    self.cities_processed += 1
                    self.publish_progress()
                return result

            city_restaurants_deals = {}
            city_deals_count = 0

            for restaurant in restaurants:
                if self.stop_event.is_set():
                    result["success"] = False
                    result["error"] = "Task was stopped"
                    break
                
                restaurant_name = restaurant.get('name')

                with self.counter_lock:
                    self.current_restaurant = restaurant_name

                deals = self.fetch_deals(city, restaurant)

                city_restaurants_deals[restaurant_name] = deals
                city_deals_count += len(deals)

                with self.counter_lock:
                    self.restaurants_processed += 1
                    self.deals_found += len(deals)
                    self.publish_progress()  

                if city not in self.deals_by_restaurant:
                    self.deals_by_restaurant[city] = {}
                self.deals_by_restaurant[city][restaurant_name] = deals

                time.sleep(self.request_delay)

            if not self.stop_event.is_set():
                self.write_city_markdown(city, city_restaurants_deals)
                self.write_city_json(city, city_restaurants_deals)

            with self.counter_lock:
                self.cities_processed += 1
                self.publish_progress()

            result["restaurants_processed"] = len(restaurants)
            result["deals_found"] = city_deals_count
            
            return result
            
        except Exception as e:
            error_msg = f"Error processing city {city}: {str(e)}"
            self.logger.error(error_msg)
            result["success"] = False
            result["error"] = error_msg
            return result
    
    def scrape_deals(
        self,
        cities_to_scrape: Optional[List[str]] = None,
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
        self.cities_processed = 0
        self.restaurants_processed = 0
        self.deals_found = 0
        self.total_cities = 0
        self.total_restaurants = 0
        self.current_city = ""
        self.current_restaurant = ""
        self.error = None

        self.cities = []
        self.restaurants_by_city = {}
        self.deals_by_restaurant = {}
        
        self.publish_progress(force=True)
        
        try:
            if cities_to_scrape:
                cities_to_process = cities_to_scrape
                self.logger.info(f"Processing specified cities: {cities_to_process}")
            else:
                self.status = "fetching_cities"
                self.publish_progress(force=True)
                
                self.cities = self.fetch_cities()
                cities_to_process = self.cities
                self.logger.info(f"Fetched {len(cities_to_process)} cities to process")
            
            self.total_cities = len(cities_to_process)
            
            if self.total_cities == 0:
                error_msg = "No cities to process"
                self.logger.error(error_msg)
                self.status = "failed"
                self.error = error_msg
                self.publish_progress(force=True)
                return {
                    "status": "failed",
                    "error": error_msg,
                    "cities_processed": 0,
                    "restaurants_processed": 0,
                    "deals_found": 0,
                    "execution_time_seconds": time.time() - self.start_time
                }

            self.status = "scraping_deals"
            self.progress = 10.0
            self.publish_progress(force=True)

            for city in cities_to_process:
                if self.stop_event.is_set():
                    self.logger.info("Deal scraping stopped by user request")
                    break
                    
                city_result = self.process_city(city)
                
                if not city_result["success"]:
                    self.logger.error(f"Failed to process city {city}: {city_result['error']}")

            if not self.stop_event.is_set():
                self._create_summary_files()

            if self.stop_event.is_set():
                self.status = "stopped"
                self.progress = 95.0
            else:
                self.status = "completed"
                self.progress = 100.0
            
            self.publish_progress(force=True)

            execution_time = time.time() - self.start_time
            
            results = {
                "status": self.status,
                "cities_processed": self.cities_processed,
                "restaurants_processed": self.restaurants_processed,
                "deals_found": self.deals_found,
                "total_cities": self.total_cities,
                "execution_time_seconds": execution_time,
                "output_directory": self.output_dir,
                "cities_data": self.deals_by_restaurant,
                "error": self.error
            }
            
            self.logger.info(
                f"Deal scraping completed. Processed {self.cities_processed} cities, "
                f"{self.restaurants_processed} restaurants, found {self.deals_found} deals in {execution_time:.2f} seconds."
            )
            
            return results
            
        except Exception as e:
            error_msg = f"Error in deal scraping workflow: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            
            self.status = "failed"
            self.error = error_msg
            self.publish_progress(force=True)
            
            return {
                "status": "failed",
                "error": error_msg,
                "cities_processed": self.cities_processed,
                "restaurants_processed": self.restaurants_processed,
                "deals_found": self.deals_found,
                "execution_time_seconds": time.time() - self.start_time
            }
    
    def _create_summary_files(self):
        try:
            summary_dir = os.path.join(self.output_dir, "city_summaries")

            overall_summary = {
                "scraping_session": {
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "total_cities": self.total_cities,
                    "cities_processed": self.cities_processed,
                    "total_restaurants": self.restaurants_processed,
                    "total_deals": self.deals_found,
                    "execution_time_seconds": time.time() - self.start_time
                },
                "city_breakdown": {}
            }

            for city, restaurants in self.deals_by_restaurant.items():
                city_deals = sum(len(deals) for deals in restaurants.values())
                overall_summary["city_breakdown"][city] = {
                    "restaurants": len(restaurants),
                    "deals": city_deals
                }

            summary_file = os.path.join(summary_dir, "overall_summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(overall_summary, f, indent=2, ensure_ascii=False)

            summary_md = os.path.join(summary_dir, "overall_summary.md")
            with open(summary_md, 'w', encoding='utf-8') as f:
                f.write("# Deal Scraping Session Summary\n\n")
                f.write(f"**Completed at:** {time.strftime('%Y-%m-%d %H:%M:%S')} (PKT)\n\n")
                f.write(f"## Overall Statistics\n\n")
                f.write(f"- **Cities Processed:** {self.cities_processed}/{self.total_cities}\n")
                f.write(f"- **Total Restaurants:** {self.restaurants_processed}\n")
                f.write(f"- **Total Deals Found:** {self.deals_found}\n")
                f.write(f"- **Execution Time:** {(time.time() - self.start_time):.2f} seconds\n\n")
                
                f.write("## City Breakdown\n\n")
                f.write("| City | Restaurants | Deals |\n")
                f.write("|------|-------------|-------|\n")
                
                for city, breakdown in overall_summary["city_breakdown"].items():
                    f.write(f"| {city} | {breakdown['restaurants']} | {breakdown['deals']} |\n")
            
            self.logger.info(f"Created summary files in {summary_dir}")
            
        except Exception as e:
            self.logger.error(f"Error creating summary files: {str(e)}")
    
    def stop(self) -> bool:
        self.logger.info("Stopping deal scraping process...")
        self.stop_event.set()
        time.sleep(1)
        
        return True
    
    def get_status(self) -> Dict[str, Any]:
        return {
            'status': self.status,
            'progress': self.progress,
            'cities_processed': self.cities_processed,
            'restaurants_processed': self.restaurants_processed,
            'deals_found': self.deals_found,
            'total_cities': self.total_cities,
            'total_restaurants': self.total_restaurants,
            'current_city': self.current_city,
            'current_restaurant': self.current_restaurant,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0,
            'error': self.error
        }
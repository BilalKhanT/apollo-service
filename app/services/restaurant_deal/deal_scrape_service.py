import requests
import json
import time
import os
import threading
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Tuple, Callable, Union
from datetime import datetime
import pytz

class DealScrapperService:
    """
    Enhanced Deal Scrapper Service with real-time progress tracking and reporting.
    Scrapes restaurant deals from Peekaboo API and saves them as markdown files.
    Follows the same patterns as other services in the Apollo system.
    """
    
    # Base configuration
    BASE_HEADERS = {
        'Ownerkey': '109fcf39f8ada91483dcced4632b0d42'
    }

    # API endpoints
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
        """
        Initialize the Deal Scrapper Service with enhanced progress tracking.

        Args:
            country: Country to fetch deals for
            language: Language for results
            output_dir: Directory to save markdown files
            max_workers: Maximum number of concurrent workers
            request_delay: Delay between API requests in seconds
            progress_update_interval: How often to update progress
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.country = country
        self.language = language
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.request_delay = request_delay
        self.progress_update_interval = progress_update_interval
        
        # Status tracking
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
        
        # Data storage
        self.cities = []
        self.restaurants_by_city = {}
        self.deals_by_restaurant = {}
        
        # Thread safety
        self.counter_lock = threading.Lock()
        self.stop_event = threading.Event()
        
        # For task manager integration
        self.task_id = None
        self.task_manager = None
        
        # For callback function
        self.progress_callback = None
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.logger.info(f"DealScrapperService initialized with output_dir={output_dir}, max_workers={max_workers}")
    
    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("DealScrapperService")
        logger.setLevel(logging.INFO)
        
        # Only add handler if not already added
        if not logger.handlers:
            # Create console handler
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def set_task_id(self, task_id: str) -> None:
        """
        Set task ID for integration with a task manager.
        
        Args:
            task_id: Task ID to use for progress reporting
        """
        self.task_id = task_id
        self.logger.info(f"Task ID set to {task_id}")
    
    def set_progress_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        Set a callback function for progress reporting.
        
        Args:
            callback: Function that accepts a dictionary of progress information
        """
        self.progress_callback = callback
        self.logger.info("Progress callback function set")
    
    def publish_progress(self, force: bool = False) -> None:
        """
        Publish current progress to task manager and/or callback function.
        
        Args:
            force: Force publish even if interval conditions not met
        """
        # Only publish if we meet the update interval or force is True
        should_update = (
            force or 
            (self.cities_processed % self.progress_update_interval == 0) or
            (self.restaurants_processed % (self.progress_update_interval * 5) == 0)
        )
        
        if not should_update:
            return
        
        # Calculate progress (5-95% during processing)
        if self.total_cities > 0:
            city_progress = (self.cities_processed / self.total_cities) * 90.0
            self.progress = 5.0 + city_progress
        
        # Build progress info dictionary
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
        
        # Send to task manager if available
        if self.task_id:
            try:
                # Try to import and use the task manager
                # This is done here to avoid circular imports
                from app.utils.task_manager import task_manager
                self.task_manager = task_manager
                
                # Update task status with progress and partial results
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
                
                # Add log entry for significant progress milestones
                if (self.cities_processed % 5 == 0 or force) and self.task_manager:
                    task_manager.publish_log(
                        self.task_id,
                        f"Deal scraping progress: {self.cities_processed}/{self.total_cities} cities processed, "
                        f"{self.restaurants_processed} restaurants, {self.deals_found} deals found, "
                        f"progress: {self.progress:.1f}%",
                        "info"
                    )
            except (ImportError, AttributeError, Exception) as e:
                self.logger.warning(f"Could not update task manager: {str(e)}")
        
        # Send to callback function if available
        if self.progress_callback:
            try:
                self.progress_callback(progress_info)
            except Exception as e:
                self.logger.warning(f"Error in progress callback: {str(e)}")
        
        # Always log progress for significant milestones or on force
        if self.cities_processed % 5 == 0 or force:
            self.logger.info(
                f"Progress: {self.cities_processed}/{self.total_cities} cities processed, "
                f"{self.restaurants_processed} restaurants, {self.deals_found} deals found, "
                f"{self.progress:.1f}%"
            )
    
    def sanitize_filename(self, name: str) -> str:
        """Convert a string to a safe filename."""
        return name.replace('/', '-').replace('\\', '-').replace(' ', '-')
    
    def fetch_cities(self) -> List[str]:
        """Fetch available cities from the API."""
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
                # Extract only the city names from the response
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
        """
        Fetch restaurants for a specific city.

        Args:
            city: City name to fetch restaurants for

        Returns:
            List of restaurant data dictionaries
        """
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
                    # Extract all relevant information for each restaurant
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
        """
        Fetch deals for a specific restaurant in a city.

        Args:
            city: City name
            restaurant: Restaurant data dictionary

        Returns:
            List of deals for the restaurant
        """
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
        """
        Write all restaurant deals for a city to a single markdown file.

        Args:
            city: City name
            restaurants_deals: Dictionary mapping restaurant names to their deals
        """
        file_path = os.path.join(self.output_dir, f"{self.sanitize_filename(city)}_deals.md")

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"# Restaurant Deals in {city}\n\n")
            f.write(f"*Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}*\n\n")

            # Check if any restaurants have deals
            if not restaurants_deals:
                f.write("No restaurant deals found for this city.\n")
                return

            # Table of contents
            f.write("## Table of Contents\n\n")
            for restaurant_name in restaurants_deals.keys():
                if restaurants_deals[restaurant_name]:  # Only add restaurants with deals
                    anchor = restaurant_name.lower().replace(' ', '-')
                    f.write(f"- [{restaurant_name}](#{anchor})\n")
            f.write("\n---\n\n")

            # Write all restaurant deals
            for restaurant_name, deals in restaurants_deals.items():
                if not deals:  # Skip restaurants with no deals
                    continue

                f.write(f"## {restaurant_name}\n\n")

                for i, deal in enumerate(deals, 1):
                    f.write(f"### Deal {i}: {deal['title']}\n")
                    f.write(f"- **Deal ID:** {deal['dealId']}\n")
                    f.write(f"- **Start Date:** {deal['startDate']}\n")
                    f.write(f"- **End Date:** {deal['endDate']}\n")
                    f.write(f"- **Powered By:** {deal['poweredBy']}\n")
                    f.write(f"- **Percentage Value:** {deal['percentageValue']}\n")
                    f.write(f"- **Description:** {deal['description']}\n")
                    f.write(f"- **Redeemable:** {deal['isRedeemable']}\n")
                    f.write(f"- **Target Branches:** {', '.join(deal['targetBranches'])}\n")
                    f.write(f"- **Associations:** {', '.join(deal['associations'])}\n")
                    f.write("\n---\n\n")
        
        self.logger.debug(f"Saved deals for {city} to {file_path}")
    
    def process_city(self, city: str) -> Dict[str, Any]:
        """
        Process a single city - fetch restaurants and deals.
        
        Args:
            city: City name to process
            
        Returns:
            Dictionary with processing results
        """
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
            
            # Update current city
            with self.counter_lock:
                self.current_city = city
                self.publish_progress()
            
            # Fetch restaurants for this city
            restaurants = self.fetch_restaurants(city)
            self.restaurants_by_city[city] = restaurants
            
            if not restaurants:
                self.logger.warning(f"No restaurants found for {city}")
                return result
            
            # Initialize city deals dictionary
            city_restaurants_deals = {}
            city_deals_count = 0
            
            # Process each restaurant
            for restaurant in restaurants:
                if self.stop_event.is_set():
                    result["success"] = False
                    result["error"] = "Task was stopped"
                    break
                
                restaurant_name = restaurant.get('name')
                
                # Update current restaurant
                with self.counter_lock:
                    self.current_restaurant = restaurant_name
                
                # Fetch deals for this restaurant
                deals = self.fetch_deals(city, restaurant)
                
                # Add to city deals dictionary
                city_restaurants_deals[restaurant_name] = deals
                city_deals_count += len(deals)
                
                # Update counters
                with self.counter_lock:
                    self.restaurants_processed += 1
                    self.deals_found += len(deals)
                    
                    # Publish progress periodically
                    if self.restaurants_processed % 10 == 0:
                        self.publish_progress()
                
                # Store deals in memory
                if city not in self.deals_by_restaurant:
                    self.deals_by_restaurant[city] = {}
                self.deals_by_restaurant[city][restaurant_name] = deals
                
                # Add delay to prevent overwhelming the API
                time.sleep(self.request_delay)
            
            # Write all deals for this city to a single markdown file
            if not self.stop_event.is_set():
                self.write_city_markdown(city, city_restaurants_deals)
            
            # Update result
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
        """
        Run the complete deal scraping workflow with enhanced progress tracking.
        
        Args:
            cities_to_scrape: List of cities to scrape (if None, fetch all available cities)
            task_id: Task ID for integration with task manager
            callback: Function to call with progress updates
            
        Returns:
            Dictionary with scraping results
        """
        # Set up task ID and callback
        if task_id:
            self.set_task_id(task_id)
        
        if callback:
            self.set_progress_callback(callback)
        
        # Initialize tracking
        self.start_time = time.time()
        self.status = "initializing"
        self.progress = 5.0  # Start at 5%
        self.cities_processed = 0
        self.restaurants_processed = 0
        self.deals_found = 0
        self.total_cities = 0
        self.total_restaurants = 0
        self.current_city = ""
        self.current_restaurant = ""
        self.error = None
        
        # Clear previous data
        self.cities = []
        self.restaurants_by_city = {}
        self.deals_by_restaurant = {}
        
        self.publish_progress(force=True)
        
        try:
            # Step 1: Determine cities to process
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
            
            # Step 2: Process cities
            self.status = "scraping_deals"
            self.progress = 10.0
            self.publish_progress(force=True)
            
            # Process cities sequentially to respect API rate limits
            for city in cities_to_process:
                if self.stop_event.is_set():
                    self.logger.info("Deal scraping stopped by user request")
                    break
                    
                city_result = self.process_city(city)
                
                # Update counters
                with self.counter_lock:
                    self.cities_processed += 1
                    self.publish_progress()
                
                if not city_result["success"]:
                    self.logger.error(f"Failed to process city {city}: {city_result['error']}")
            
            # Step 3: Finalize
            if self.stop_event.is_set():
                self.status = "stopped"
                self.progress = 95.0
            else:
                self.status = "completed"
                self.progress = 100.0
            
            self.publish_progress(force=True)
            
            # Prepare final results
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
    
    def stop(self) -> bool:
        """Stop the deal scraping process gracefully."""
        self.logger.info("Stopping deal scraping process...")
        self.stop_event.set()
        return True
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the deal scrapper."""
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
import logging
import os
import time
import requests
import threading
import json
from typing import Dict, Any, Optional, Callable
from datetime import datetime
import pytz


class RestaurantDealsScraperService:
    """
    Enhanced RestaurantDealsScraperService with progress tracking and task manager integration.
    Integrated with Apollo's task management and WebSocket system.
    """

    BASE_HEADERS = {
        'Ownerkey': '109fcf39f8ada91483dcced4632b0d42'
    }
    CITIES_URL = 'https://secure-sdk.peekaboo.guru/klaoshcjanaij2ktnbjkmiasvtafoabxtenstn5'
    RESTAURANTS_URL = 'https://secure-sdk.peekaboo.guru/uljin2s3nitoi89njkhklgkj5'
    DEALS_URL = 'https://secure-sdk.peekaboo.guru/ksbolruuahrndcjchshjhejgjhasdo787kjieo767kjsgeskoyfgwwhkl6'

    def __init__(self, cities_list=None, country="Pakistan", language="en", output_dir="restaurant_deals"):
        """
        Initialize the RestaurantDealsScraperService with enhanced progress tracking.
        
        Args:
            cities_list: List of specific cities to scrape. If None or empty, will fetch all cities
            country: Target country for scraping
            language: Language for API requests
            output_dir: Directory to save scraped data
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.cities_list = cities_list if cities_list else []
        self.country = country
        self.language = language
        self.output_dir = output_dir
        
        # Data storage
        self.cities = []
        self.restaurants_by_city = {}
        self.deals_by_restaurant = {}
        
        # Status tracking
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.cities_processed = 0
        self.restaurants_processed = 0
        self.deals_processed = 0
        self.total_cities = 0
        self.total_restaurants = 0
        self.current_city = ""
        self.current_restaurant = ""
        self.error = None
        
        # Thread safety
        self.counter_lock = threading.Lock()
        
        # For task manager integration
        self.task_id = None
        self.task_manager = None
        
        # For callback function
        self.progress_callback = None
        
        # Stop flag
        self.stop_requested = False
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        cities_info = f"specific cities: {self.cities_list}" if self.cities_list else "all available cities"
        self.logger.info(f"RestaurantDealsScraperService initialized for {country} in {language} - Target: {cities_info}")

    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("RestaurantDealsScraperService")
        logger.setLevel(logging.INFO)
        
        # Only add handler if not already added
        if not logger.handlers:
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
        Uses estimation-based progress calculation as requested.
        
        Args:
            force: Force publish even if conditions not met
        """
        # Estimation-based progress calculation
        if self.status == "fetching_cities":
            # Increment slowly until we know how many cities we have
            self.progress = min(10.0, self.progress + 0.5)
        elif self.status == "using_specified_cities":
            self.progress = 10.0
        elif self.status == "fetching_restaurants":
            if self.total_cities > 0:
                # 10% to 50% based on cities processed
                city_progress = (self.cities_processed / self.total_cities) * 40.0
                self.progress = 10.0 + city_progress
            else:
                # Increment slowly if we don't know total
                self.progress = min(50.0, self.progress + 0.2)
        elif self.status == "fetching_deals":
            if self.total_restaurants > 0:
                # 50% to 85% based on restaurants processed
                deals_progress = (self.restaurants_processed / self.total_restaurants) * 35.0
                self.progress = 50.0 + deals_progress
            else:
                # Increment slowly if we don't know total
                self.progress = min(85.0, self.progress + 0.1)
        elif self.status == "writing_files":
            # 85% to 95% based on cities processed
            if self.total_cities > 0:
                file_progress = (self.cities_processed / self.total_cities) * 10.0
                self.progress = 85.0 + file_progress
            else:
                self.progress = min(95.0, self.progress + 0.5)
        elif self.status == "generating_summary":
            # Stay at 95% until complete
            self.progress = 95.0
        elif self.status == "completed":
            self.progress = 100.0
        elif self.status == "failed" or self.status == "stopped":
            # Don't change progress for error states
            pass
        else:
            # Default increment for unknown states
            self.progress = min(95.0, self.progress + 0.1)
        
        # Cap progress at 95% until completion as requested
        if self.status not in ["completed", "failed", "stopped"] and self.progress > 95.0:
            self.progress = 95.0
        
        # Build progress info dictionary
        progress_info = {
            "status": self.status,
            "progress": self.progress,
            "cities_processed": self.cities_processed,
            "restaurants_processed": self.restaurants_processed,
            "deals_processed": self.deals_processed,
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
                from app.utils.task_manager import task_manager
                self.task_manager = task_manager
                
                # Update task status with progress and partial results
                task_manager.update_task_status(
                    self.task_id,
                    progress=self.progress,
                    result={
                        "restaurant_partial_results": {
                            "cities_processed": self.cities_processed,
                            "restaurants_processed": self.restaurants_processed,
                            "deals_processed": self.deals_processed,
                            "total_cities": self.total_cities,
                            "total_restaurants": self.total_restaurants,
                            "current_city": self.current_city,
                            "current_restaurant": self.current_restaurant
                        }
                    }
                )
                
                # Add log entry for progress updates
                if force or self.cities_processed % 2 == 0:
                    task_manager.publish_log(
                        self.task_id,
                        f"Restaurant scraping progress: {self.cities_processed}/{self.total_cities} cities, "
                        f"{self.restaurants_processed} restaurants, {self.deals_processed} deals processed, "
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

    def sanitize_filename(self, name):
        """Sanitize filename for safe file operations."""
        return name.replace('/', '-').replace('\\', '-').replace(' ', '-')

    def fetch_cities(self):
        """Fetch all available cities with progress tracking."""
        self.status = "fetching_cities"
        self.publish_progress(force=True)
        
        if self.task_id:
            try:
                from app.utils.task_manager import task_manager
                task_manager.publish_log(self.task_id, f"Fetching all available cities for {self.country}", "info")
            except:
                pass
        
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
                self.cities = [item['city'] for item in response.json()]
                self.total_cities = len(self.cities)
                self.logger.info(f"Found {len(self.cities)} available cities from API")
                
                if self.task_id:
                    try:
                        from app.utils.task_manager import task_manager
                        task_manager.publish_log(self.task_id, f"Found {len(self.cities)} available cities from API", "info")
                    except:
                        pass
                
                self.publish_progress(force=True)
                return self.cities
            else:
                error_msg = f"Error fetching cities: {response.status_code}"
                self.logger.error(error_msg)
                self.error = error_msg
                return []

        except Exception as e:
            error_msg = f"Exception while fetching cities: {str(e)}"
            self.logger.error(error_msg)
            self.error = error_msg
            return []

    def fetch_restaurants(self, city):
        """Fetch restaurants for a city with progress tracking."""
        if self.stop_requested:
            return []
            
        self.current_city = city
        self.status = "fetching_restaurants"
        
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
                    if self.stop_requested:
                        break
                        
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

                self.logger.info(f"Found {len(city_restaurants)} restaurants in {city}")
                return city_restaurants
            else:
                self.logger.warning(f"Failed to fetch restaurants for {city}: {response.status_code}")
                return []

        except Exception as e:
            self.logger.error(f"Exception fetching restaurants for {city}: {str(e)}")
            return []

    def fetch_deals(self, city, restaurant):
        """Fetch deals for a restaurant with progress tracking."""
        if self.stop_requested:
            return []
            
        entity_id = restaurant.get('entityId')
        restaurant_name = restaurant.get('name')
        self.current_restaurant = restaurant_name

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
                self.logger.warning(f"Error fetching deals for {restaurant_name}: {response.status_code}")
                return []

            deals = response.json()
            results = []

            for deal in deals:
                if self.stop_requested:
                    break
                    
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

            with self.counter_lock:
                self.deals_processed += len(results)
                self.restaurants_processed += 1
                
            self.publish_progress()
            return results

        except Exception as e:
            self.logger.error(f"Exception fetching deals for {restaurant_name}: {str(e)}")
            return []

    def write_city_markdown(self, city, restaurants_deals):
        """Write city data to markdown file."""
        if self.stop_requested:
            return False
            
        file_path = os.path.join(self.output_dir, f"{self.sanitize_filename(city)}_deals.md")

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"# Restaurant Deals in {city}\n\n")
                f.write(f"*Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}*\n\n")

                if not restaurants_deals:
                    f.write("No restaurant deals found for this city.\n")
                    return True

                f.write("## Table of Contents\n\n")
                for restaurant_name in restaurants_deals.keys():
                    if restaurants_deals[restaurant_name]: 
                        anchor = restaurant_name.lower().replace(' ', '-')
                        f.write(f"- [{restaurant_name}](#{anchor})\n")
                f.write("\n---\n\n")

                for restaurant_name, deals in restaurants_deals.items():
                    if not deals: 
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
            
            return True
        except Exception as e:
            self.logger.error(f"Error writing file for {city}: {str(e)}")
            return False

    def scrape_all_deals(
        self,
        task_id: Optional[str] = None,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Dict[str, Any]:
        """
        Main method to scrape all restaurant deals with progress tracking.
        Uses estimation-based progress calculation as requested.
        
        Args:
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
        self.progress = 0.0
        self.cities_processed = 0
        self.restaurants_processed = 0
        self.deals_processed = 0
        self.total_cities = 0
        self.total_restaurants = 0
        self.current_city = ""
        self.current_restaurant = ""
        self.error = None
        self.stop_requested = False
        
        self.publish_progress(force=True)
        
        try:
            # Step 1: Determine cities to process
            if self.cities_list and len(self.cities_list) > 0:
                # Use specified cities directly
                self.cities = self.cities_list
                self.total_cities = len(self.cities)
                self.status = "using_specified_cities"
                self.progress = 10.0
                
                if self.task_id:
                    try:
                        from app.utils.task_manager import task_manager
                        task_manager.publish_log(
                            self.task_id, 
                            f"Using specified cities: {', '.join(self.cities)} ({len(self.cities)} cities)", 
                            "info"
                        )
                    except:
                        pass
                
                self.logger.info(f"Using specified cities: {', '.join(self.cities)} ({len(self.cities)} cities)")
                self.publish_progress(force=True)
            else:
                # Fetch all available cities
                self.logger.info("No specific cities provided, fetching all available cities")
                if self.task_id:
                    try:
                        from app.utils.task_manager import task_manager
                        task_manager.publish_log(self.task_id, "No specific cities provided, fetching all available cities", "info")
                    except:
                        pass
                
                cities = self.fetch_cities()
                if not cities or self.stop_requested:
                    self.status = "failed" if self.error else "stopped"
                    self.publish_progress(force=True)
                    return self._create_result_dict()
            
            # Step 2: Process each city
            for city in self.cities:
                if self.stop_requested:
                    break
                    
                self.current_city = city
                self.status = "fetching_restaurants"
                
                # Fetch restaurants for this city
                restaurants = self.fetch_restaurants(city)
                if not restaurants:
                    with self.counter_lock:
                        self.cities_processed += 1
                    continue
                
                self.restaurants_by_city[city] = restaurants
                self.total_restaurants += len(restaurants)
                
                # Update progress after getting restaurants
                self.publish_progress()
                
                # Step 3: Fetch deals for each restaurant
                self.status = "fetching_deals"
                city_deals = {}
                
                for restaurant in restaurants:
                    if self.stop_requested:
                        break
                        
                    restaurant_name = restaurant.get('name')
                    deals = self.fetch_deals(city, restaurant)
                    city_deals[restaurant_name] = deals
                    
                    # Small delay to prevent overwhelming the API
                    time.sleep(0.1)
                
                self.deals_by_restaurant[city] = city_deals
                
                # Step 4: Write markdown file for this city
                self.status = "writing_files"
                self.write_city_markdown(city, city_deals)
                
                with self.counter_lock:
                    self.cities_processed += 1
                
                self.publish_progress()
                
                if self.task_id:
                    try:
                        from app.utils.task_manager import task_manager
                        task_manager.publish_log(
                            self.task_id, 
                            f"Completed processing {city}: {len(restaurants)} restaurants, "
                            f"{sum(len(deals) for deals in city_deals.values())} deals", 
                            "info"
                        )
                    except:
                        pass
            
            # Step 5: Prepare database summary
            self.status = "generating_summary"
            database_summary = self.prepare_database_summary()
            
            # Final completion - set to 100% as requested
            if self.stop_requested:
                self.status = "stopped"
            else:
                self.status = "completed"
                self.progress = 100.0  # Only now set to 100%
            
            self.publish_progress(force=True)
            
            if self.task_id:
                try:
                    from app.utils.task_manager import task_manager
                    task_manager.publish_log(
                        self.task_id, 
                        f"Restaurant scraping completed: {self.cities_processed} cities, "
                        f"{self.restaurants_processed} restaurants, {self.deals_processed} deals", 
                        "info"
                    )
                except:
                    pass
            
            # Return the final results with database summary
            result = self._create_result_dict()
            result["database_summary"] = database_summary
            return result
            
        except Exception as e:
            error_msg = f"Error in scrape_all_deals: {str(e)}"
            self.logger.error(error_msg)
            self.error = error_msg
            self.status = "failed"
            self.publish_progress(force=True)
            
            if self.task_id:
                try:
                    from app.utils.task_manager import task_manager
                    task_manager.publish_log(self.task_id, error_msg, "error")
                except:
                    pass
            
            return self._create_result_dict()

    def _create_result_dict(self) -> Dict[str, Any]:
        """Create standardized result dictionary."""
        return {
            "status": self.status,
            "country": self.country,
            "language": self.language,
            "cities_requested": self.cities_list,
            "cities_processed": self.cities_processed,
            "restaurants_processed": self.restaurants_processed,
            "deals_processed": self.deals_processed,
            "total_cities": self.total_cities,
            "total_restaurants": self.total_restaurants,
            "execution_time_seconds": time.time() - self.start_time,
            "output_directory": self.output_dir,
            "error": self.error
        }

    def prepare_database_summary(self) -> Dict[str, Any]:
        """Prepare summary data for database storage."""
        if self.stop_requested:
            return {}
            
        try:
            summary = {
                "country": self.country,
                "language": self.language,
                "cities_requested": self.cities_list,
                "scrape_date": datetime.now(pytz.timezone("Asia/Karachi")).strftime('%Y-%m-%d %H:%M:%S'),
                "total_cities": len(self.cities),
                "cities_processed": self.cities_processed,
                "total_restaurants": self.total_restaurants,
                "restaurants_processed": self.restaurants_processed,
                "total_deals": self.deals_processed,
                "execution_time_seconds": time.time() - self.start_time,
                "status": self.status,
                "cities": self.cities,
                "restaurants_by_city": {
                    city: len(restaurants) for city, restaurants in self.restaurants_by_city.items()
                },
                "deals_by_city": {
                    city: sum(len(deals) for deals in city_deals.values()) 
                    for city, city_deals in self.deals_by_restaurant.items()
                }
            }
            
            self.logger.info("Database summary prepared successfully")
            return summary
            
        except Exception as e:
            self.logger.error(f"Error preparing database summary: {str(e)}")
            return {}

    def stop(self) -> bool:
        """Stop the scraping process gracefully."""
        self.logger.info("Stop requested for restaurant scraping")
        self.stop_requested = True
        
        if self.task_id:
            try:
                from app.utils.task_manager import task_manager
                task_manager.publish_log(self.task_id, "Stop requested - gracefully stopping restaurant scraper", "info")
            except:
                pass
        
        return True
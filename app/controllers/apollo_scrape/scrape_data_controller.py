# import logging
# import asyncio
# from pathlib import Path
# from typing import Dict, Any, Optional

# logger = logging.getLogger(__name__)

# class ScrapeDataController:

#     MAX_RETRIES = 3
    
#     # WEB SCRAPER CLEANUP
    
#     @staticmethod
#     async def web_scraper_cleanup(task_id: str, scraped: str) -> bool:
#         try:
#             base_path = Path("apollo_data")

#             files_result = await ScrapeDataController.cleanup_scraped_files(task_id, base_path, scraped)
#             pdfs_result = await ScrapeDataController.cleanup_downloaded_files(task_id, base_path, scraped)

#             await ScrapeDataController._cleanup_empty_directories(task_id, base_path)

#             if files_result["success"] and pdfs_result["success"]:
#                 total_deleted = files_result["deleted_count"] + pdfs_result["deleted_count"]
#                 logger.info(f"Complete cleanup successful for task_id: {task_id}")
#                 logger.info(f"Total files deleted: {total_deleted} (Files: {files_result['deleted_count']}, Downloads: {pdfs_result['deleted_count']})")
#                 return True
#             else:
#                 total_errors = len(files_result["errors"]) + len(pdfs_result["errors"])
#                 logger.warning(f"Cleanup completed with {total_errors} errors for task_id: {task_id}")
#                 return False
                
#         except Exception as e:
#             logger.error(f"Unexpected error during cleanup coordination for task_id {task_id}: {str(e)}")
#             return False

#     @staticmethod
#     async def cleanup_scraped_files(task_id: str, base_path: Path, scraped: str) -> dict:
#         try:
#             scraped_dir = base_path / "scraped" / task_id
#             metadata_dir = base_path / "metadata" / task_id / "files"
            
#             deleted_files = []
#             errors = []
            
#             if not scraped_dir.exists():
#                 logger.info(f"Scraped directory not found: {scraped_dir} - skipping files cleanup")
#                 return {
#                     "success": True,
#                     "deleted_count": 0,
#                     "deleted_files": [],
#                     "errors": [],
#                     "skipped": True
#                 }
            
#             logger.info(f"Processing scraped files directory: {scraped_dir}")
            
#             for subdir in scraped_dir.iterdir():
#                 if subdir.is_dir():
#                     logger.info(f"Processing scraped subdirectory: {subdir.name}")

#                     md_files = list(subdir.glob("*.md"))
                    
#                     for md_file in md_files:
#                         try:
#                             file_stem = md_file.stem

#                             api_success = False
                            
#                             if metadata_dir.exists():
#                                 metadata_file = metadata_dir / f"{file_stem}.meta"
                                    
#                                 if metadata_file.exists():
#                                     metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
                                        
#                                     if metadata:
#                                         api_success = await ScrapeDataController._send_file_to_momento_api(
#                                             md_file, 
#                                             metadata, 
#                                             subdir.name
#                                         )
#                                     else:
#                                         logger.warning(f"Could not parse metadata for {md_file.name}")
#                                 else:
#                                     logger.warning(f"Scrape metadata file not found: {metadata_file}")

#                             md_file.unlink()
#                             deleted_files.append(str(md_file))
                            
                            
#                             if api_success:
#                                 logger.info(f"Deleted MD file after successful API upload: {md_file}")
#                             else:
#                                 logger.info(f"Deleted MD file (API upload failed/skipped): {md_file}")
                            
#                             if metadata_dir.exists():
#                                 metadata_file = metadata_dir / f"{file_stem}.meta"
                                
#                                 if metadata_file.exists():
#                                     metadata_file.unlink()
#                                     deleted_files.append(str(metadata_file))
#                                     logger.info(f"Deleted scrape metadata file: {metadata_file}")
#                                 else:
#                                     logger.warning(f"Scrape metadata file not found: {metadata_file}")
                                    
#                         except Exception as e:
#                             error_msg = f"Error processing scraped file {md_file}: {str(e)}"
#                             logger.error(error_msg)
#                             errors.append(error_msg)

#                     try:
#                         if subdir.exists() and not any(subdir.iterdir()):
#                             subdir.rmdir()
#                             logger.info(f"Removed empty scraped subdirectory: {subdir}")
#                     except Exception as e:
#                         error_msg = f"Error removing scraped subdirectory {subdir}: {str(e)}"
#                         logger.error(error_msg)
#                         errors.append(error_msg)

#             return {
#                 "success": len(errors) == 0,
#                 "deleted_count": len(deleted_files),
#                 "deleted_files": deleted_files,
#                 "errors": errors,
#                 "skipped": False
#             }
            
#         except Exception as e:
#             error_msg = f"Unexpected error during scraped files cleanup: {str(e)}"
#             logger.error(error_msg)
#             return {
#                 "success": False,
#                 "deleted_count": 0,
#                 "deleted_files": [],
#                 "errors": [error_msg],
#                 "skipped": False
#             }

#     @staticmethod
#     async def cleanup_downloaded_files(task_id: str, base_path: Path, scraped: str) -> dict:
#         try:
#             download_dir = base_path / "downloads" / task_id 
#             download_meta_data_dir = base_path / "metadata" / task_id / "document"
            
#             deleted_files = []
#             errors = []
            
#             if not download_dir.exists():
#                 logger.info(f"Download directory not found: {download_dir} - skipping downloads cleanup")
#                 return {
#                     "success": True,
#                     "deleted_count": 0,
#                     "deleted_files": [],
#                     "errors": [],
#                     "skipped": True
#                 }
            
#             logger.info(f"Processing download files directory: {download_dir}")
            
#             for subdir in download_dir.iterdir():
#                 if subdir.is_dir():
#                     logger.info(f"Processing download subdirectory: {subdir.name}")

#                     download_files = (
#                         list(subdir.glob("*.pdf")) + 
#                         list(subdir.glob("*.xlsx")) + 
#                         list(subdir.glob("*.xls"))
#                     )
                    
#                     for download_file in download_files:
#                         try:
#                             file_stem = download_file.stem

#                             api_success = False
#                             if download_meta_data_dir.exists():
#                                 download_metadata_file = download_meta_data_dir / f"{file_stem}.meta"
                                    
#                                 if download_metadata_file.exists():
#                                     metadata = await ScrapeDataController._parse_metadata_file(download_metadata_file, scraped)
                                        
#                                     if metadata:
#                                         api_success = await ScrapeDataController._send_file_to_momento_api(
#                                             download_file, 
#                                             metadata, 
#                                             subdir.name
#                                         )
#                                     else:
#                                         logger.warning(f"Could not parse metadata for {download_file.name}")
#                                 else:
#                                     logger.warning(f"Download metadata file not found: {download_metadata_file}")

#                             download_file.unlink()
#                             deleted_files.append(str(download_file))
                            
#                             if api_success:
#                                 logger.info(f"Deleted download file after successful API upload: {download_file}")
#                             else:
#                                 logger.info(f"Deleted download file (API upload failed/skipped): {download_file}")

#                             if download_meta_data_dir.exists():
#                                 download_metadata_file = download_meta_data_dir / f"{file_stem}.meta"
                                
#                                 if download_metadata_file.exists():
#                                     download_metadata_file.unlink()
#                                     deleted_files.append(str(download_metadata_file))
#                                     logger.info(f"Deleted download metadata file: {download_metadata_file}")
#                                 else:
#                                     logger.warning(f"Download metadata file not found: {download_metadata_file}")
                                    
#                         except Exception as e:
#                             error_msg = f"Error processing download file {download_file}: {str(e)}"
#                             logger.error(error_msg)
#                             errors.append(error_msg)

#                     try:
#                         if subdir.exists() and not any(subdir.iterdir()):
#                             subdir.rmdir()
#                             logger.info(f"Removed empty download subdirectory: {subdir}")
#                     except Exception as e:
#                         error_msg = f"Error removing download subdirectory {subdir}: {str(e)}"
#                         logger.error(error_msg)
#                         errors.append(error_msg)

#             return {
#                 "success": len(errors) == 0,
#                 "deleted_count": len(deleted_files),
#                 "deleted_files": deleted_files,
#                 "errors": errors,
#                 "skipped": False
#             }
            
#         except Exception as e:
#             error_msg = f"Unexpected error during download files cleanup: {str(e)}"
#             logger.error(error_msg)
#             return {
#                 "success": False,
#                 "deleted_count": 0,
#                 "deleted_files": [],
#                 "errors": [error_msg],
#                 "skipped": False
#             }

#     @staticmethod
#     async def _cleanup_empty_directories(task_id: str, base_path: Path) -> None:
#         try:
#             scraped_task_dir = base_path / "scraped" / task_id
#             if scraped_task_dir.exists() and not any(scraped_task_dir.iterdir()):
#                 scraped_task_dir.rmdir()
#                 logger.info(f"Removed empty scraped task directory: {scraped_task_dir}")

#             download_task_dir = base_path / "downloads" / task_id
#             if download_task_dir.exists() and not any(download_task_dir.iterdir()):
#                 download_task_dir.rmdir()
#                 logger.info(f"Removed empty download task directory: {download_task_dir}")

#             metadata_files_dir = base_path / "metadata" / task_id / "files"
#             if metadata_files_dir.exists() and not any(metadata_files_dir.iterdir()):
#                 metadata_files_dir.rmdir()
#                 logger.info(f"Removed empty metadata files directory: {metadata_files_dir}")

#             metadata_document_dir = base_path / "metadata" / task_id / "document"
#             if metadata_document_dir.exists() and not any(metadata_document_dir.iterdir()):
#                 metadata_document_dir.rmdir()
#                 logger.info(f"Removed empty metadata document directory: {metadata_document_dir}")

#             metadata_task_dir = base_path / "metadata" / task_id
#             if metadata_task_dir.exists() and not any(metadata_task_dir.iterdir()):
#                 metadata_task_dir.rmdir()
#                 logger.info(f"Removed empty metadata task directory: {metadata_task_dir}")
                    
#         except Exception as e:
#             logger.error(f"Error cleaning up empty directories: {str(e)}")

#     @staticmethod
#     async def fb_scraper_cleanup(task_id: str, scraped: str) -> bool:
#         try:
#             base_path = Path("apollo_data")

#             files_result = await ScrapeDataController.cleanup_fb_scraped_files(task_id, base_path, scraped)

#             await ScrapeDataController._cleanup_empty_fb_directories(task_id, base_path)

#             if files_result["success"]:
#                 total_deleted = files_result["deleted_count"]
#                 logger.info(f"Complete cleanup successful for task_id: {task_id}")
#                 logger.info(f"Total files deleted: {total_deleted} (Files: {files_result['deleted_count']})")
#                 return True
#             else:
#                 total_errors = len(files_result["errors"])
#                 logger.warning(f"Cleanup completed with {total_errors} errors for task_id: {task_id}")
#                 return False
                
#         except Exception as e:
#             logger.error(f"Unexpected error during cleanup coordination for task_id {task_id}: {str(e)}")
#             return False
    
#     # FACEBOOK SCRAPER CLEANUP

#     @staticmethod
#     async def cleanup_fb_scraped_files(task_id: str, base_path: Path, scraped: str) -> dict:
#         try:
#             task_path = base_path / "facebook" / task_id
#             metadata_dir = task_path / "metadata"

#             deleted_files = []
#             errors = []

#             if not task_path.exists():
#                 logger.warning(f"Task directory not found: {task_path}")
#                 return {
#                     "success": True,
#                     "deleted_count": 0,
#                     "deleted_files": [],
#                     "errors": [],
#                     "skipped": True
#                 }

#             keyword_dirs = [d for d in task_path.iterdir() if d.is_dir() and d.name != "metadata"]
#             logger.info(f"Processing keyword directories: {[d.name for d in keyword_dirs]}")

#             for keyword_dir in keyword_dirs:
#                 for md_file in keyword_dir.rglob("*.md"):
#                     try:
#                         file_stem = md_file.stem
#                         api_success = False

#                         metadata_file = metadata_dir / f"{file_stem}.meta"
#                         if metadata_file.exists():
#                             metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
#                             if metadata:
#                                 api_success = await ScrapeDataController._send_file_to_momento_api(
#                                     md_file,
#                                     metadata,
#                                     keyword_dir.name
#                                 )
#                             else:
#                                 logger.warning(f"Could not parse metadata for {md_file.name}")
#                         else:
#                             logger.warning(f"Scrape metadata file not found: {metadata_file}")

#                         md_file.unlink()
#                         deleted_files.append(str(md_file))
#                         logger.info(f"Deleted MD file {'after successful API upload' if api_success else '(API upload failed/skipped)'}: {md_file}")

#                         if metadata_file.exists():
#                             metadata_file.unlink()
#                             deleted_files.append(str(metadata_file))
#                             logger.info(f"Deleted scrape metadata file: {metadata_file}")

#                     except Exception as e:
#                         error_msg = f"Error processing scraped file {md_file}: {str(e)}"
#                         logger.error(error_msg)
#                         errors.append(error_msg)

#             return {
#                 "success": len(errors) == 0,
#                 "deleted_count": len(deleted_files),
#                 "deleted_files": deleted_files,
#                 "errors": errors,
#                 "skipped": False
#             }

#         except Exception as e:
#             error_msg = f"Unexpected error during scraped files cleanup: {str(e)}"
#             logger.error(error_msg)
#             return {
#                 "success": False,
#                 "deleted_count": 0,
#                 "deleted_files": [],
#                 "errors": [error_msg],
#                 "skipped": False
#             }
        
#     @staticmethod
#     async def _cleanup_empty_fb_directories(task_id: str, base_path: Path) -> None:
#         try:
#             task_path = base_path / "facebook" / task_id

#             if task_path.exists():
#                 for subdir in task_path.iterdir():
#                     if subdir.is_dir() and subdir.name != "metadata":
#                         if not any(subdir.rglob("*")):
#                             subdir.rmdir()
#                             logger.info(f"Removed empty keyword directory: {subdir}")

#             metadata_dir = task_path / "metadata"
#             if metadata_dir.exists() and not any(metadata_dir.rglob("*")):
#                 metadata_dir.rmdir()
#                 logger.info(f"Removed empty metadata directory: {metadata_dir}")

#             if task_path.exists() and not any(task_path.rglob("*")):
#                 task_path.rmdir()
#                 logger.info(f"Removed empty task directory: {task_path}")

#         except Exception as e:
#             logger.error(f"Error cleaning up empty Facebook directories: {str(e)}")

#     @staticmethod
#     async def _parse_metadata_file(metadata_file_path: Path, scraped: str) -> Optional[Dict[str, Any]]:
#         try:
#             if not metadata_file_path.exists():
#                 logger.warning(f"Metadata file does not exist: {metadata_file_path}")
#                 return None

#             file_size = metadata_file_path.stat().st_size
#             if file_size == 0:
#                 logger.warning(f"Metadata file is empty: {metadata_file_path}")
#                 return None

#             with open(metadata_file_path, 'r', encoding='utf-8') as f:
#                 content = f.read().strip()
                
#             if not content:
#                 logger.warning(f"Metadata file contains no content: {metadata_file_path}")
#                 return None

#             metadata = {}
            
#             try:
#                 for line in content.split('\n'):
#                     line = line.strip()
#                     if line and ':' in line:
#                         key, value = line.split(':', 1)
#                         key = key.strip()
#                         value = value.strip()
#                         metadata[key] = value
                    
#                 if metadata:
#                     logger.debug(f"Successfully parsed key-value metadata for: {metadata_file_path.name}")
#                 else:
#                     logger.warning(f"No valid key-value pairs found in: {metadata_file_path}")
#                     return None
                        
#             except Exception as e:
#                 logger.error(f"Failed to parse key-value format in {metadata_file_path}: {str(e)}")
#                 return None

#             if not isinstance(metadata, dict):
#                 logger.error(f"Metadata file contains invalid data type {type(metadata)}: {metadata_file_path}")
#                 return None

#             parsed_metadata = {
#                 "bot_id": metadata.get("bot_id", "unknown"),
#                 "document_id": metadata.get("document_id", "unknown"), 
#                 "document_name": metadata.get("document_name", metadata_file_path.stem),
#                 "document_url": metadata.get("document_url", ""),
#                 "expiry": metadata.get("expiry", "none"),
#                 "source": metadata.get("source", "website"),
#                 "checksum": metadata.get("checksum", ""),
#                 "scraped_at": scraped
#             }
            
#             logger.debug(f"Successfully parsed metadata for: {metadata_file_path.name}")
#             return parsed_metadata
            
#         except Exception as e:
#             logger.error(f"Unexpected error parsing metadata file {metadata_file_path}: {str(e)}")
#             return None

#     @staticmethod
#     def _get_file_type(file_path: Path) -> str:
#         return file_path.suffix.lower().lstrip('.')

#     @staticmethod
#     async def _send_file_to_momento_api(
#         file_path: Path, 
#         metadata: Dict[str, Any], 
#         task_name: str
#     ) -> bool:
#         metadata["file_type"] = ScrapeDataController._get_file_type(file_path)
#         metadata["topic_name"] = task_name
        
#         for attempt in range(ScrapeDataController.MAX_RETRIES):
#             try:
#                 with open(file_path, 'rb') as f:
#                     file_content = f.read()

#                 payload = {
#                     "metadata": metadata,
#                     "file": file_content.hex()  
#                 }

#                 success = await ScrapeDataController._mock_api_call(payload)
                
#                 if success:
#                     logger.info(f"Successfully sent {file_path.name} to Momento API")
#                     return True
#                 else:
#                     logger.warning(f"Failed to send {file_path.name} to Momento API (attempt {attempt + 1})")
                    
#             except Exception as e:
#                 logger.error(f"Error sending {file_path.name} to Momento API (attempt {attempt + 1}): {str(e)}")

#             if attempt < ScrapeDataController.MAX_RETRIES - 1:
#                 await asyncio.sleep(1)
        
#         logger.error(f"Failed to send {file_path.name} to Momento API after {ScrapeDataController.MAX_RETRIES} attempts")
#         return False

#     @staticmethod
#     async def _mock_api_call(payload: Dict[str, Any]) -> bool:
#         await asyncio.sleep(2)
#         return True
        
import logging
import asyncio
import json
import zipfile
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class ScrapeDataController:

    MAX_RETRIES = 3
    
    # WEB SCRAPER CLEANUP 
    
    @staticmethod
    async def web_scraper_cleanup(task_id: str, scraped: str) -> bool:
        try:
            base_path = Path("apollo_data")

            files_data = await ScrapeDataController._collect_web_scraped_data(task_id, base_path, scraped)
            
            if not files_data["files"]:
                logger.info(f"No files found for task_id: {task_id} - skipping batch processing")
                await ScrapeDataController._cleanup_empty_directories(task_id, base_path)
                return True

            batch_success = await ScrapeDataController._create_and_send_batch(
                task_id, files_data["files"], files_data["metadata_list"], scraped
            )

            cleanup_result = await ScrapeDataController._cleanup_processed_files(
                task_id, base_path, files_data["files"]
            )

            await ScrapeDataController._cleanup_empty_directories(task_id, base_path)

            if batch_success and cleanup_result["success"]:
                logger.info(f"Complete batch processing successful for task_id: {task_id}")
                logger.info(f"Total files processed: {len(files_data['files'])}")
                return True
            else:
                logger.warning(f"Batch processing completed with issues for task_id: {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error during batch processing for task_id {task_id}: {str(e)}")
            return False

    @staticmethod
    async def fb_scraper_cleanup(task_id: str, scraped: str) -> bool:
        try:
            base_path = Path("apollo_data")

            files_data = await ScrapeDataController._collect_fb_scraped_data(task_id, base_path, scraped)
            
            if not files_data["files"]:
                logger.info(f"No files found for task_id: {task_id} - skipping batch processing")
                await ScrapeDataController._cleanup_empty_fb_directories(task_id, base_path)
                return True

            batch_success = await ScrapeDataController._create_and_send_batch(
                task_id, files_data["files"], files_data["metadata_list"], scraped
            )

            cleanup_result = await ScrapeDataController._cleanup_processed_fb_files(
                task_id, base_path, files_data["files"]
            )

            await ScrapeDataController._cleanup_empty_fb_directories(task_id, base_path)

            if batch_success and cleanup_result["success"]:
                logger.info(f"Complete batch processing successful for task_id: {task_id}")
                logger.info(f"Total files processed: {len(files_data['files'])}")
                return True
            else:
                logger.warning(f"Batch processing completed with issues for task_id: {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error during batch processing for task_id {task_id}: {str(e)}")
            return False


    @staticmethod
    async def _collect_web_scraped_data(task_id: str, base_path: Path, scraped: str) -> Dict[str, Any]:
        files = []
        metadata_list = []
        
        try:
            scraped_files = await ScrapeDataController._collect_scraped_files(task_id, base_path, scraped)
            files.extend(scraped_files["files"])
            metadata_list.extend(scraped_files["metadata_list"])
            
            download_files = await ScrapeDataController._collect_downloaded_files(task_id, base_path, scraped)
            files.extend(download_files["files"])
            metadata_list.extend(download_files["metadata_list"])
            
            logger.info(f"Collected {len(files)} files for batch processing - task_id: {task_id}")
            
        except Exception as e:
            logger.error(f"Error collecting web scraped data for task_id {task_id}: {str(e)}")
            
        return {
            "files": files,
            "metadata_list": metadata_list
        }

    @staticmethod
    async def _collect_fb_scraped_data(task_id: str, base_path: Path, scraped: str) -> Dict[str, Any]:
        files = []
        metadata_list = []
        
        try:
            task_path = base_path / "facebook" / task_id
            metadata_dir = task_path / "metadata"

            if not task_path.exists():
                logger.warning(f"Task directory not found: {task_path}")
                return {"files": [], "metadata_list": []}

            keyword_dirs = [d for d in task_path.iterdir() if d.is_dir() and d.name != "metadata"]
            logger.info(f"Processing keyword directories: {[d.name for d in keyword_dirs]}")

            for keyword_dir in keyword_dirs:
                for md_file in keyword_dir.rglob("*.md"):
                    try:
                        file_stem = md_file.stem
                        metadata_file = metadata_dir / f"{file_stem}.meta"
                        
                        if metadata_file.exists():
                            metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
                            if metadata:
                                metadata["topic_name"] = keyword_dir.name
                                metadata["file_type"] = ScrapeDataController._get_file_type(md_file)
                                
                                files.append({
                                    "file_path": md_file,
                                    "metadata_path": metadata_file,
                                    "topic_name": keyword_dir.name
                                })
                                metadata_list.append(metadata)
                            else:
                                logger.warning(f"Could not parse metadata for {md_file.name}")
                        else:
                            logger.warning(f"Metadata file not found: {metadata_file}")
                            
                    except Exception as e:
                        logger.error(f"Error processing file {md_file}: {str(e)}")

            logger.info(f"Collected {len(files)} Facebook files for batch processing - task_id: {task_id}")
            
        except Exception as e:
            logger.error(f"Error collecting Facebook scraped data for task_id {task_id}: {str(e)}")
            
        return {
            "files": files,
            "metadata_list": metadata_list
        }

    @staticmethod
    async def _collect_scraped_files(task_id: str, base_path: Path, scraped: str) -> Dict[str, Any]:
        files = []
        metadata_list = []
        
        scraped_dir = base_path / "scraped" / task_id
        metadata_dir = base_path / "metadata" / task_id / "files"
        
        if not scraped_dir.exists():
            logger.info(f"Scraped directory not found: {scraped_dir}")
            return {"files": [], "metadata_list": []}
        
        logger.info(f"Processing scraped files directory: {scraped_dir}")
        
        for subdir in scraped_dir.iterdir():
            if subdir.is_dir():
                md_files = list(subdir.glob("*.md"))
                
                for md_file in md_files:
                    try:
                        file_stem = md_file.stem
                        metadata_file = metadata_dir / f"{file_stem}.meta"
                        
                        if metadata_dir.exists() and metadata_file.exists():
                            metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
                            if metadata:
                                metadata["topic_name"] = subdir.name
                                metadata["file_type"] = ScrapeDataController._get_file_type(md_file)
                                
                                files.append({
                                    "file_path": md_file,
                                    "metadata_path": metadata_file,
                                    "topic_name": subdir.name
                                })
                                metadata_list.append(metadata)
                            else:
                                logger.warning(f"Could not parse metadata for {md_file.name}")
                        else:
                            logger.warning(f"Metadata file not found: {metadata_file}")
                            
                    except Exception as e:
                        logger.error(f"Error processing scraped file {md_file}: {str(e)}")
        
        return {"files": files, "metadata_list": metadata_list}

    @staticmethod
    async def _collect_downloaded_files(task_id: str, base_path: Path, scraped: str) -> Dict[str, Any]:
        files = []
        metadata_list = []
        
        download_dir = base_path / "downloads" / task_id 
        download_meta_data_dir = base_path / "metadata" / task_id / "document"
        
        if not download_dir.exists():
            logger.info(f"Download directory not found: {download_dir}")
            return {"files": [], "metadata_list": []}
        
        logger.info(f"Processing download files directory: {download_dir}")
        
        for subdir in download_dir.iterdir():
            if subdir.is_dir():
                download_files = (
                    list(subdir.glob("*.pdf")) + 
                    list(subdir.glob("*.xlsx")) + 
                    list(subdir.glob("*.xls"))
                )
                
                for download_file in download_files:
                    try:
                        file_stem = download_file.stem
                        download_metadata_file = download_meta_data_dir / f"{file_stem}.meta"
                        
                        if download_meta_data_dir.exists() and download_metadata_file.exists():
                            metadata = await ScrapeDataController._parse_metadata_file(download_metadata_file, scraped)
                            if metadata:
                                metadata["topic_name"] = subdir.name
                                metadata["file_type"] = ScrapeDataController._get_file_type(download_file)
                                
                                files.append({
                                    "file_path": download_file,
                                    "metadata_path": download_metadata_file,
                                    "topic_name": subdir.name
                                })
                                metadata_list.append(metadata)
                            else:
                                logger.warning(f"Could not parse metadata for {download_file.name}")
                        else:
                            logger.warning(f"Download metadata file not found: {download_metadata_file}")
                            
                    except Exception as e:
                        logger.error(f"Error processing download file {download_file}: {str(e)}")
        
        return {"files": files, "metadata_list": metadata_list}

    @staticmethod
    async def _create_and_send_batch(
        task_id: str, 
        files: List[Dict[str, Any]], 
        metadata_list: List[Dict[str, Any]], 
        scraped: str
    ) -> bool:
        if not files:
            logger.warning(f"No files to process for task_id: {task_id}")
            return True
            
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                zip_path = temp_path / f"{task_id}_batch.zip"
                metadata_json_path = temp_path / "metadata.json"

                batch_metadata = {
                    "scraped_at": scraped,
                    "total_files": len(files),
                    "files_metadata": metadata_list
                }

                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:

                    zip_structure = {"metadata.json": "Batch metadata and file mapping"}
                    for i, file_info in enumerate(files):
                        file_path = file_info["file_path"]
                        zip_name = f"{file_info['topic_name']}/{file_path.name}"
                        zipf.write(file_path, zip_name)

                        metadata_list[i]["zip_file_path"] = zip_name
                        metadata_list[i]["original_filename"] = file_path.name
                        metadata_list[i]["file_index"] = i
                        metadata_list[i]["file_size_bytes"] = file_path.stat().st_size

                        zip_structure[zip_name] = f"Content file ({metadata_list[i]['file_type']})"
                        
                        logger.debug(f"Added {file_path.name} to ZIP as {zip_name}")

                    batch_metadata["zip_structure"] = zip_structure

                    with open(metadata_json_path, 'w', encoding='utf-8') as f:
                        json.dump(batch_metadata, f, indent=2, ensure_ascii=False)
                    zipf.write(metadata_json_path, "metadata.json")

                success = await ScrapeDataController._send_batch_to_momento_api(
                    zip_path, batch_metadata, task_id
                )
                
                if success:
                    logger.info(f"Successfully sent batch ZIP for task_id: {task_id}")
                    return True
                else:
                    logger.error(f"Failed to send batch ZIP for task_id: {task_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error creating and sending batch for task_id {task_id}: {str(e)}")
            return False

    @staticmethod
    async def _send_batch_to_momento_api(
        zip_path: Path, 
        batch_metadata: Dict[str, Any], 
        task_id: str
    ) -> bool:
        for attempt in range(ScrapeDataController.MAX_RETRIES):
            try:
                with open(zip_path, 'rb') as f:
                    zip_content = f.read()

                payload = {
                    "batch_metadata": batch_metadata,
                    "zip_file": zip_content.hex()
                }

                print(payload)

                success = await ScrapeDataController._mock_batch_api_call(payload)
                
                if success:
                    logger.info(f"Successfully sent batch ZIP to Momento API - task_id: {task_id}")
                    return True
                else:
                    logger.warning(f"Failed to send batch ZIP to Momento API (attempt {attempt + 1}) - task_id: {task_id}")
                    
            except Exception as e:
                logger.error(f"Error sending batch ZIP to Momento API (attempt {attempt + 1}) - task_id: {task_id}: {str(e)}")

            if attempt < ScrapeDataController.MAX_RETRIES - 1:
                await asyncio.sleep(2)
        
        logger.error(f"Failed to send batch ZIP to Momento API after {ScrapeDataController.MAX_RETRIES} attempts - task_id: {task_id}")
        return False

    @staticmethod
    async def _cleanup_processed_files(task_id: str, base_path: Path, files: List[Dict[str, Any]]) -> Dict[str, Any]:
        deleted_files = []
        errors = []
        
        try:
            for file_info in files:
                try:
                    file_path = file_info["file_path"]
                    if file_path.exists():
                        file_path.unlink()
                        deleted_files.append(str(file_path))
                        logger.debug(f"Deleted file: {file_path}")

                    metadata_path = file_info["metadata_path"]
                    if metadata_path.exists():
                        metadata_path.unlink()
                        deleted_files.append(str(metadata_path))
                        logger.debug(f"Deleted metadata: {metadata_path}")
                        
                except Exception as e:
                    error_msg = f"Error deleting files for {file_info.get('file_path', 'unknown')}: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)
            
            logger.info(f"Cleaned up {len(deleted_files)} files for task_id: {task_id}")
            
        except Exception as e:
            error_msg = f"Unexpected error during file cleanup for task_id {task_id}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
        
        return {
            "success": len(errors) == 0,
            "deleted_count": len(deleted_files),
            "deleted_files": deleted_files,
            "errors": errors
        }

    @staticmethod
    async def _cleanup_processed_fb_files(task_id: str, base_path: Path, files: List[Dict[str, Any]]) -> Dict[str, Any]:
        return await ScrapeDataController._cleanup_processed_files(task_id, base_path, files)

    @staticmethod
    async def _cleanup_empty_directories(task_id: str, base_path: Path) -> None:
        try:
            scraped_task_dir = base_path / "scraped" / task_id
            if scraped_task_dir.exists() and not any(scraped_task_dir.iterdir()):
                scraped_task_dir.rmdir()
                logger.info(f"Removed empty scraped task directory: {scraped_task_dir}")

            download_task_dir = base_path / "downloads" / task_id
            if download_task_dir.exists() and not any(download_task_dir.iterdir()):
                download_task_dir.rmdir()
                logger.info(f"Removed empty download task directory: {download_task_dir}")

            metadata_files_dir = base_path / "metadata" / task_id / "files"
            if metadata_files_dir.exists() and not any(metadata_files_dir.iterdir()):
                metadata_files_dir.rmdir()
                logger.info(f"Removed empty metadata files directory: {metadata_files_dir}")

            metadata_document_dir = base_path / "metadata" / task_id / "document"
            if metadata_document_dir.exists() and not any(metadata_document_dir.iterdir()):
                metadata_document_dir.rmdir()
                logger.info(f"Removed empty metadata document directory: {metadata_document_dir}")

            metadata_task_dir = base_path / "metadata" / task_id
            if metadata_task_dir.exists() and not any(metadata_task_dir.iterdir()):
                metadata_task_dir.rmdir()
                logger.info(f"Removed empty metadata task directory: {metadata_task_dir}")
                    
        except Exception as e:
            logger.error(f"Error cleaning up empty directories: {str(e)}")

    @staticmethod
    async def _cleanup_empty_fb_directories(task_id: str, base_path: Path) -> None:
        try:
            task_path = base_path / "facebook" / task_id

            if task_path.exists():
                for subdir in task_path.iterdir():
                    if subdir.is_dir() and subdir.name != "metadata":
                        if not any(subdir.rglob("*")):
                            subdir.rmdir()
                            logger.info(f"Removed empty keyword directory: {subdir}")

            metadata_dir = task_path / "metadata"
            if metadata_dir.exists() and not any(metadata_dir.rglob("*")):
                metadata_dir.rmdir()
                logger.info(f"Removed empty metadata directory: {metadata_dir}")

            if task_path.exists() and not any(task_path.rglob("*")):
                task_path.rmdir()
                logger.info(f"Removed empty task directory: {task_path}")

        except Exception as e:
            logger.error(f"Error cleaning up empty Facebook directories: {str(e)}")

    @staticmethod
    async def _parse_metadata_file(metadata_file_path: Path, scraped: str) -> Optional[Dict[str, Any]]:
        try:
            if not metadata_file_path.exists():
                logger.warning(f"Metadata file does not exist: {metadata_file_path}")
                return None

            file_size = metadata_file_path.stat().st_size
            if file_size == 0:
                logger.warning(f"Metadata file is empty: {metadata_file_path}")
                return None

            with open(metadata_file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                
            if not content:
                logger.warning(f"Metadata file contains no content: {metadata_file_path}")
                return None

            metadata = {}
            
            try:
                for line in content.split('\n'):
                    line = line.strip()
                    if line and ':' in line:
                        key, value = line.split(':', 1)
                        key = key.strip()
                        value = value.strip()
                        metadata[key] = value
                    
                if metadata:
                    logger.debug(f"Successfully parsed key-value metadata for: {metadata_file_path.name}")
                else:
                    logger.warning(f"No valid key-value pairs found in: {metadata_file_path}")
                    return None
                        
            except Exception as e:
                logger.error(f"Failed to parse key-value format in {metadata_file_path}: {str(e)}")
                return None

            if not isinstance(metadata, dict):
                logger.error(f"Metadata file contains invalid data type {type(metadata)}: {metadata_file_path}")
                return None

            parsed_metadata = {
                "bot_id": metadata.get("bot_id", "unknown"),
                "document_id": metadata.get("document_id", "unknown"), 
                "document_name": metadata.get("document_name", metadata_file_path.stem),
                "document_url": metadata.get("document_url", ""),
                "expiry": metadata.get("expiry", "none"),
                "source": metadata.get("source", "website"),
                "checksum": metadata.get("checksum", ""),
                "scraped_at": scraped
            }
            
            logger.debug(f"Successfully parsed metadata for: {metadata_file_path.name}")
            return parsed_metadata
            
        except Exception as e:
            logger.error(f"Unexpected error parsing metadata file {metadata_file_path}: {str(e)}")
            return None

    @staticmethod
    def _get_file_type(file_path: Path) -> str:
        return file_path.suffix.lower().lstrip('.')

    @staticmethod
    async def _mock_batch_api_call(payload: Dict[str, Any]) -> bool:
        await asyncio.sleep(3)  
        return True
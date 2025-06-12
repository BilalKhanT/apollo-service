import logging
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ScrapeDataController:

    MAX_RETRIES = 3
    
    # WEB SCRAPER CLEANUP
    
    @staticmethod
    async def web_scraper_cleanup(task_id: str, scraped: str) -> bool:
        try:
            base_path = Path("apollo_data")

            files_result = await ScrapeDataController.cleanup_scraped_files(task_id, base_path, scraped)
            pdfs_result = await ScrapeDataController.cleanup_downloaded_files(task_id, base_path, scraped)

            await ScrapeDataController._cleanup_empty_directories(task_id, base_path)

            if files_result["success"] and pdfs_result["success"]:
                total_deleted = files_result["deleted_count"] + pdfs_result["deleted_count"]
                logger.info(f"Complete cleanup successful for task_id: {task_id}")
                logger.info(f"Total files deleted: {total_deleted} (Files: {files_result['deleted_count']}, Downloads: {pdfs_result['deleted_count']})")
                return True
            else:
                total_errors = len(files_result["errors"]) + len(pdfs_result["errors"])
                logger.warning(f"Cleanup completed with {total_errors} errors for task_id: {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error during cleanup coordination for task_id {task_id}: {str(e)}")
            return False

    @staticmethod
    async def cleanup_scraped_files(task_id: str, base_path: Path, scraped: str) -> dict:
        try:
            scraped_dir = base_path / "scraped" / task_id
            metadata_dir = base_path / "metadata" / task_id / "files"
            
            deleted_files = []
            errors = []
            
            if not scraped_dir.exists():
                logger.info(f"Scraped directory not found: {scraped_dir} - skipping files cleanup")
                return {
                    "success": True,
                    "deleted_count": 0,
                    "deleted_files": [],
                    "errors": [],
                    "skipped": True
                }
            
            logger.info(f"Processing scraped files directory: {scraped_dir}")
            
            for subdir in scraped_dir.iterdir():
                if subdir.is_dir():
                    logger.info(f"Processing scraped subdirectory: {subdir.name}")

                    md_files = list(subdir.glob("*.md"))
                    
                    for md_file in md_files:
                        try:
                            file_stem = md_file.stem

                            api_success = False
                            
                            if metadata_dir.exists():
                                metadata_file = metadata_dir / f"{file_stem}.meta"
                                    
                                if metadata_file.exists():
                                    metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
                                        
                                    if metadata:
                                        api_success = await ScrapeDataController._send_file_to_momento_api(
                                            md_file, 
                                            metadata, 
                                            subdir.name
                                        )
                                    else:
                                        logger.warning(f"Could not parse metadata for {md_file.name}")
                                else:
                                    logger.warning(f"Scrape metadata file not found: {metadata_file}")

                            md_file.unlink()
                            deleted_files.append(str(md_file))
                            
                            
                            if api_success:
                                logger.info(f"Deleted MD file after successful API upload: {md_file}")
                            else:
                                logger.info(f"Deleted MD file (API upload failed/skipped): {md_file}")
                            
                            if metadata_dir.exists():
                                metadata_file = metadata_dir / f"{file_stem}.meta"
                                
                                if metadata_file.exists():
                                    metadata_file.unlink()
                                    deleted_files.append(str(metadata_file))
                                    logger.info(f"Deleted scrape metadata file: {metadata_file}")
                                else:
                                    logger.warning(f"Scrape metadata file not found: {metadata_file}")
                                    
                        except Exception as e:
                            error_msg = f"Error processing scraped file {md_file}: {str(e)}"
                            logger.error(error_msg)
                            errors.append(error_msg)

                    try:
                        if subdir.exists() and not any(subdir.iterdir()):
                            subdir.rmdir()
                            logger.info(f"Removed empty scraped subdirectory: {subdir}")
                    except Exception as e:
                        error_msg = f"Error removing scraped subdirectory {subdir}: {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)

            return {
                "success": len(errors) == 0,
                "deleted_count": len(deleted_files),
                "deleted_files": deleted_files,
                "errors": errors,
                "skipped": False
            }
            
        except Exception as e:
            error_msg = f"Unexpected error during scraped files cleanup: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "deleted_count": 0,
                "deleted_files": [],
                "errors": [error_msg],
                "skipped": False
            }

    @staticmethod
    async def cleanup_downloaded_files(task_id: str, base_path: Path, scraped: str) -> dict:
        try:
            download_dir = base_path / "downloads" / task_id 
            download_meta_data_dir = base_path / "metadata" / task_id / "document"
            
            deleted_files = []
            errors = []
            
            if not download_dir.exists():
                logger.info(f"Download directory not found: {download_dir} - skipping downloads cleanup")
                return {
                    "success": True,
                    "deleted_count": 0,
                    "deleted_files": [],
                    "errors": [],
                    "skipped": True
                }
            
            logger.info(f"Processing download files directory: {download_dir}")
            
            for subdir in download_dir.iterdir():
                if subdir.is_dir():
                    logger.info(f"Processing download subdirectory: {subdir.name}")

                    download_files = (
                        list(subdir.glob("*.pdf")) + 
                        list(subdir.glob("*.xlsx")) + 
                        list(subdir.glob("*.xls"))
                    )
                    
                    for download_file in download_files:
                        try:
                            file_stem = download_file.stem

                            api_success = False
                            if download_meta_data_dir.exists():
                                download_metadata_file = download_meta_data_dir / f"{file_stem}.meta"
                                    
                                if download_metadata_file.exists():
                                    metadata = await ScrapeDataController._parse_metadata_file(download_metadata_file, scraped)
                                        
                                    if metadata:
                                        api_success = await ScrapeDataController._send_file_to_momento_api(
                                            download_file, 
                                            metadata, 
                                            subdir.name
                                        )
                                    else:
                                        logger.warning(f"Could not parse metadata for {download_file.name}")
                                else:
                                    logger.warning(f"Download metadata file not found: {download_metadata_file}")

                            download_file.unlink()
                            deleted_files.append(str(download_file))
                            
                            if api_success:
                                logger.info(f"Deleted download file after successful API upload: {download_file}")
                            else:
                                logger.info(f"Deleted download file (API upload failed/skipped): {download_file}")

                            if download_meta_data_dir.exists():
                                download_metadata_file = download_meta_data_dir / f"{file_stem}.meta"
                                
                                if download_metadata_file.exists():
                                    download_metadata_file.unlink()
                                    deleted_files.append(str(download_metadata_file))
                                    logger.info(f"Deleted download metadata file: {download_metadata_file}")
                                else:
                                    logger.warning(f"Download metadata file not found: {download_metadata_file}")
                                    
                        except Exception as e:
                            error_msg = f"Error processing download file {download_file}: {str(e)}"
                            logger.error(error_msg)
                            errors.append(error_msg)

                    try:
                        if subdir.exists() and not any(subdir.iterdir()):
                            subdir.rmdir()
                            logger.info(f"Removed empty download subdirectory: {subdir}")
                    except Exception as e:
                        error_msg = f"Error removing download subdirectory {subdir}: {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)

            return {
                "success": len(errors) == 0,
                "deleted_count": len(deleted_files),
                "deleted_files": deleted_files,
                "errors": errors,
                "skipped": False
            }
            
        except Exception as e:
            error_msg = f"Unexpected error during download files cleanup: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "deleted_count": 0,
                "deleted_files": [],
                "errors": [error_msg],
                "skipped": False
            }

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
    async def fb_scraper_cleanup(task_id: str, scraped: str) -> bool:
        try:
            base_path = Path("apollo_data")

            files_result = await ScrapeDataController.cleanup_fb_scraped_files(task_id, base_path, scraped)

            await ScrapeDataController._cleanup_empty_fb_directories(task_id, base_path)

            if files_result["success"]:
                total_deleted = files_result["deleted_count"]
                logger.info(f"Complete cleanup successful for task_id: {task_id}")
                logger.info(f"Total files deleted: {total_deleted} (Files: {files_result['deleted_count']})")
                return True
            else:
                total_errors = len(files_result["errors"])
                logger.warning(f"Cleanup completed with {total_errors} errors for task_id: {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error during cleanup coordination for task_id {task_id}: {str(e)}")
            return False
    
    # FACEBOOK SCRAPER CLEANUP

    @staticmethod
    async def cleanup_fb_scraped_files(task_id: str, base_path: Path, scraped: str) -> dict:
        try:
            task_path = base_path / "facebook" / task_id
            metadata_dir = task_path / "metadata"

            deleted_files = []
            errors = []

            if not task_path.exists():
                logger.warning(f"Task directory not found: {task_path}")
                return {
                    "success": True,
                    "deleted_count": 0,
                    "deleted_files": [],
                    "errors": [],
                    "skipped": True
                }

            keyword_dirs = [d for d in task_path.iterdir() if d.is_dir() and d.name != "metadata"]
            logger.info(f"Processing keyword directories: {[d.name for d in keyword_dirs]}")

            for keyword_dir in keyword_dirs:
                for md_file in keyword_dir.rglob("*.md"):
                    try:
                        file_stem = md_file.stem
                        api_success = False

                        metadata_file = metadata_dir / f"{file_stem}.meta"
                        if metadata_file.exists():
                            metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
                            if metadata:
                                api_success = await ScrapeDataController._send_file_to_momento_api(
                                    md_file,
                                    metadata,
                                    keyword_dir.name
                                )
                            else:
                                logger.warning(f"Could not parse metadata for {md_file.name}")
                        else:
                            logger.warning(f"Scrape metadata file not found: {metadata_file}")

                        md_file.unlink()
                        deleted_files.append(str(md_file))
                        logger.info(f"Deleted MD file {'after successful API upload' if api_success else '(API upload failed/skipped)'}: {md_file}")

                        if metadata_file.exists():
                            metadata_file.unlink()
                            deleted_files.append(str(metadata_file))
                            logger.info(f"Deleted scrape metadata file: {metadata_file}")

                    except Exception as e:
                        error_msg = f"Error processing scraped file {md_file}: {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)

            return {
                "success": len(errors) == 0,
                "deleted_count": len(deleted_files),
                "deleted_files": deleted_files,
                "errors": errors,
                "skipped": False
            }

        except Exception as e:
            error_msg = f"Unexpected error during scraped files cleanup: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "deleted_count": 0,
                "deleted_files": [],
                "errors": [error_msg],
                "skipped": False
            }
        
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
    async def _send_file_to_momento_api(
        file_path: Path, 
        metadata: Dict[str, Any], 
        task_name: str
    ) -> bool:
        metadata["file_type"] = ScrapeDataController._get_file_type(file_path)
        metadata["topic_name"] = task_name
        
        for attempt in range(ScrapeDataController.MAX_RETRIES):
            try:
                with open(file_path, 'rb') as f:
                    file_content = f.read()

                payload = {
                    "metadata": metadata,
                    "file": file_content.hex()  
                }

                success = await ScrapeDataController._mock_api_call(payload)
                
                if success:
                    logger.info(f"Successfully sent {file_path.name} to Momento API")
                    return True
                else:
                    logger.warning(f"Failed to send {file_path.name} to Momento API (attempt {attempt + 1})")
                    
            except Exception as e:
                logger.error(f"Error sending {file_path.name} to Momento API (attempt {attempt + 1}): {str(e)}")

            if attempt < ScrapeDataController.MAX_RETRIES - 1:
                await asyncio.sleep(1)
        
        logger.error(f"Failed to send {file_path.name} to Momento API after {ScrapeDataController.MAX_RETRIES} attempts")
        return False

    @staticmethod
    async def _mock_api_call(payload: Dict[str, Any]) -> bool:
        await asyncio.sleep(2)
        return True
        
        
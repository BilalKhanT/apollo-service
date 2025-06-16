import logging
import asyncio
import json
import zipfile
import tempfile
import httpx
from pathlib import Path
from typing import Dict, Any, Optional, List
from app.utils.config import DOCUMENT_BULK_URL

logger = logging.getLogger(__name__)

class ScrapeDataController:

    MAX_RETRIES = 3
    STATUS_CHECK_INTERVAL = 10  
    MAX_STATUS_CHECKS = 360
    CONCURRENT_FILE_LIMIT = 10 
    
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

            batch_id = await ScrapeDataController._create_and_send_batch(
                task_id, files_data["files"], files_data["metadata_list"], scraped, "website"
            )

            if batch_id:
                success = await ScrapeDataController._wait_for_batch_completion(batch_id)
                
                if success:
                    cleanup_result = await ScrapeDataController._cleanup_processed_files(
                        task_id, base_path, files_data["files"]
                    )
                    await ScrapeDataController._cleanup_empty_directories(task_id, base_path)
                    
                    logger.info(f"Complete batch processing successful for task_id: {task_id}")
                    logger.info(f"Total files processed: {len(files_data['files'])}")
                    return True
                else:
                    logger.error(f"Batch processing failed - files not deleted for task_id: {task_id}")
                    return False
            else:
                logger.error(f"Failed to submit batch for task_id: {task_id}")
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

            batch_id = await ScrapeDataController._create_and_send_batch(
                task_id, files_data["files"], files_data["metadata_list"], scraped, "facebook"
            )

            if batch_id:
                success = await ScrapeDataController._wait_for_batch_completion(batch_id)
                
                if success:
                    cleanup_result = await ScrapeDataController._cleanup_processed_fb_files(
                        task_id, base_path, files_data["files"]
                    )
                    await ScrapeDataController._cleanup_empty_fb_directories(task_id, base_path)
                    
                    logger.info(f"Complete batch processing successful for task_id: {task_id}")
                    logger.info(f"Total files processed: {len(files_data['files'])}")
                    return True
                else:
                    logger.error(f"Batch processing failed - files not deleted for task_id: {task_id}")
                    return False
            else:
                logger.error(f"Failed to submit batch for task_id: {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error during batch processing for task_id {task_id}: {str(e)}")
            return False

    @staticmethod
    async def _collect_web_scraped_data(task_id: str, base_path: Path, scraped: str) -> Dict[str, Any]:
        files = []
        metadata_list = []
        
        try:
            scraped_task, download_task = await asyncio.gather(
                ScrapeDataController._collect_scraped_files(task_id, base_path, scraped),
                ScrapeDataController._collect_downloaded_files(task_id, base_path, scraped),
                return_exceptions=True
            )

            if not isinstance(scraped_task, Exception):
                files.extend(scraped_task["files"])
                metadata_list.extend(scraped_task["metadata_list"])
            else:
                logger.error(f"Error in scraped files collection: {scraped_task}")
            
            if not isinstance(download_task, Exception):
                files.extend(download_task["files"])
                metadata_list.extend(download_task["metadata_list"])
            else:
                logger.error(f"Error in download files collection: {download_task}")
            
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

            file_candidates = []
            for keyword_dir in keyword_dirs:
                for md_file in keyword_dir.rglob("*.md"):
                    file_stem = md_file.stem
                    metadata_file = metadata_dir / f"{file_stem}.meta"
                    
                    if metadata_file.exists():
                        file_candidates.append({
                            "md_file": md_file,
                            "metadata_file": metadata_file,
                            "keyword_dir": keyword_dir
                        })

            if file_candidates:
                semaphore = asyncio.Semaphore(ScrapeDataController.CONCURRENT_FILE_LIMIT)
                
                async def process_fb_file(candidate):
                    async with semaphore:
                        try:
                            md_file = candidate["md_file"]
                            metadata_file = candidate["metadata_file"]
                            keyword_dir = candidate["keyword_dir"]
                            
                            metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
                            if metadata:
                                metadata["task_name"] = keyword_dir.name
                                metadata["file_type"] = ScrapeDataController._get_file_type(md_file)
                                
                                return {
                                    "file_info": {
                                        "file_path": md_file,
                                        "metadata_path": metadata_file,
                                        "task_name": keyword_dir.name
                                    },
                                    "metadata": metadata
                                }
                            else:
                                logger.warning(f"Could not parse metadata for {md_file.name}")
                                return None
                        except Exception as e:
                            logger.error(f"Error processing file {candidate['md_file']}: {str(e)}")
                            return None

                results = await asyncio.gather(
                    *[process_fb_file(candidate) for candidate in file_candidates],
                    return_exceptions=True
                )

                for result in results:
                    if result is not None and not isinstance(result, Exception):
                        files.append(result["file_info"])
                        metadata_list.append(result["metadata"])

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

        file_candidates = []
        for subdir in scraped_dir.iterdir():
            if subdir.is_dir():
                md_files = list(subdir.glob("*.md"))
                
                for md_file in md_files:
                    file_stem = md_file.stem
                    metadata_file = metadata_dir / f"{file_stem}.meta"
                    
                    if metadata_dir.exists() and metadata_file.exists():
                        file_candidates.append({
                            "md_file": md_file,
                            "metadata_file": metadata_file,
                            "subdir": subdir
                        })

        if file_candidates:
            semaphore = asyncio.Semaphore(ScrapeDataController.CONCURRENT_FILE_LIMIT)
            
            async def process_scraped_file(candidate):
                async with semaphore:
                    try:
                        md_file = candidate["md_file"]
                        metadata_file = candidate["metadata_file"]
                        subdir = candidate["subdir"]
                        
                        metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
                        if metadata:
                            metadata["task_name"] = subdir.name
                            metadata["file_type"] = ScrapeDataController._get_file_type(md_file)
                            
                            return {
                                "file_info": {
                                    "file_path": md_file,
                                    "metadata_path": metadata_file,
                                    "task_name": subdir.name
                                },
                                "metadata": metadata
                            }
                        else:
                            logger.warning(f"Could not parse metadata for {md_file.name}")
                            return None
                    except Exception as e:
                        logger.error(f"Error processing scraped file {candidate['md_file']}: {str(e)}")
                        return None

            results = await asyncio.gather(
                *[process_scraped_file(candidate) for candidate in file_candidates],
                return_exceptions=True
            )

            for result in results:
                if result is not None and not isinstance(result, Exception):
                    files.append(result["file_info"])
                    metadata_list.append(result["metadata"])
        
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
        
        file_candidates = []
        for subdir in download_dir.iterdir():
            if subdir.is_dir():
                download_files = (
                    list(subdir.glob("*.pdf")) + 
                    list(subdir.glob("*.xlsx")) + 
                    list(subdir.glob("*.xls"))
                )
                
                for download_file in download_files:
                    file_stem = download_file.stem
                    download_metadata_file = download_meta_data_dir / f"{file_stem}.meta"
                    
                    if download_meta_data_dir.exists() and download_metadata_file.exists():
                        file_candidates.append({
                            "download_file": download_file,
                            "metadata_file": download_metadata_file,
                            "subdir": subdir
                        })

        if file_candidates:
            semaphore = asyncio.Semaphore(ScrapeDataController.CONCURRENT_FILE_LIMIT)
            
            async def process_download_file(candidate):
                async with semaphore:
                    try:
                        download_file = candidate["download_file"]
                        metadata_file = candidate["metadata_file"]
                        subdir = candidate["subdir"]
                        
                        metadata = await ScrapeDataController._parse_metadata_file(metadata_file, scraped)
                        if metadata:
                            metadata["task_name"] = subdir.name
                            metadata["file_type"] = ScrapeDataController._get_file_type(download_file)
                            
                            return {
                                "file_info": {
                                    "file_path": download_file,
                                    "metadata_path": metadata_file,
                                    "task_name": subdir.name
                                },
                                "metadata": metadata
                            }
                        else:
                            logger.warning(f"Could not parse metadata for {download_file.name}")
                            return None
                    except Exception as e:
                        logger.error(f"Error processing download file {candidate['download_file']}: {str(e)}")
                        return None

            results = await asyncio.gather(
                *[process_download_file(candidate) for candidate in file_candidates],
                return_exceptions=True
            )

            for result in results:
                if result is not None and not isinstance(result, Exception):
                    files.append(result["file_info"])
                    metadata_list.append(result["metadata"])
        
        return {"files": files, "metadata_list": metadata_list}
    
    @staticmethod
    async def _create_and_send_batch(
        task_id: str, 
        files: List[Dict[str, Any]], 
        metadata_list: List[Dict[str, Any]], 
        scraped: str,
        source: str
    ) -> Optional[str]:
        if not files:
            logger.warning(f"No files to process for task_id: {task_id}")
            return None
            
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                zip_path = temp_path / "documents.zip"
                metadata_json_path = temp_path / "metadata.json"

                documents = []
                filename_counter = {}

                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    
                    for i, (file_info, metadata) in enumerate(zip(files, metadata_list)):
                        file_path = file_info["file_path"]
                        original_filename = file_path.name

                        if original_filename in filename_counter:
                            filename_counter[original_filename] += 1
                            name_parts = original_filename.rsplit('.', 1)
                            if len(name_parts) == 2:
                                zip_filename = f"{name_parts[0]}_{filename_counter[original_filename]}.{name_parts[1]}"
                            else:
                                zip_filename = f"{original_filename}_{filename_counter[original_filename]}"
                        else:
                            filename_counter[original_filename] = 0
                            zip_filename = original_filename

                        zipf.write(file_path, zip_filename)
                        logger.debug(f"Added {file_path.name} to ZIP as {zip_filename}")
                        
                        document = {
                            "document_id": metadata["document_id"],
                            "document_name": zip_filename,
                            "document_url": metadata["document_url"],
                            "checksum": metadata["checksum"],
                            "file_type": metadata["file_type"],
                            "task_name": metadata["task_name"]
                        }
                        documents.append(document)

                metadata_payload = {
                    "source": source,
                    "bot_id": metadata_list[0]["bot_id"] if metadata_list else "unknown",
                    "scraped_at": scraped,
                    "documents": documents
                }
                
                with open(metadata_json_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata_payload, f, indent=2, ensure_ascii=False)

                batch_id = await ScrapeDataController._submit_batch_to_api(
                    zip_path, metadata_json_path, task_id
                )
                
                if batch_id:
                    logger.info(f"Successfully submitted batch {batch_id} for task_id: {task_id}")
                    return batch_id
                else:
                    logger.error(f"Failed to submit batch for task_id: {task_id}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error creating and sending batch for task_id {task_id}: {str(e)}")
            return None

    @staticmethod
    async def _submit_batch_to_api(zip_path: Path, metadata_json_path: Path, task_id: str) -> Optional[str]:
        
        for attempt in range(ScrapeDataController.MAX_RETRIES):
            try:
                url = f"{DOCUMENT_BULK_URL}/documents/bulk-ingest"

                files = {
                    'documents_zip': ('documents.zip', open(zip_path, 'rb'), 'application/zip'),
                    'metadata_json': ('metadata.json', open(metadata_json_path, 'rb'), 'application/json')
                }
                
                logger.info(f"Submitting batch to API (attempt {attempt + 1}) - task_id: {task_id}")
                logger.info(f"ZIP size: {zip_path.stat().st_size} bytes")
                logger.info(f"Metadata size: {metadata_json_path.stat().st_size} bytes")

                async with httpx.AsyncClient(timeout=300.0) as client:
                    response = await client.post(url, files=files)
                    
                    if response.status_code == 200:
                        result = response.json()
                        batch_id = result.get("batch_id")
                        total_docs = result.get("total_documents")
                        status = result.get("status")
                        message = result.get("message")
                        
                        logger.info(f" Batch submitted successfully:")
                        logger.info(f"   Batch ID: {batch_id}")
                        logger.info(f"   Total documents: {total_docs}")
                        logger.info(f"   Status: {status}")
                        logger.info(f"   Message: {message}")
                        
                        return batch_id
                    else:
                        error_text = response.text
                        logger.error(f" API error {response.status_code}: {error_text}")
                        
            except httpx.RequestError as e:
                logger.error(f"HTTP error (attempt {attempt + 1}): {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error (attempt {attempt + 1}): {str(e)}")
            finally:
                for file_tuple in files.values():
                    if hasattr(file_tuple[1], 'close'):
                        file_tuple[1].close()

            if attempt < ScrapeDataController.MAX_RETRIES - 1:
                await asyncio.sleep(5)
        
        logger.error(f"Failed to submit batch after {ScrapeDataController.MAX_RETRIES} attempts")
        return None

    @staticmethod
    async def _wait_for_batch_completion(batch_id: str) -> bool:        
        for check_count in range(ScrapeDataController.MAX_STATUS_CHECKS):
            try:
                url = f"{DOCUMENT_BULK_URL}/documents/bulk-ingest/{batch_id}"
                
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        result = response.json()
                        
                        batch_id_resp = result.get("batch_id")
                        total_docs = result.get("total_documents")
                        processed_docs = result.get("processed_documents")
                        failed_docs = result.get("failed_documents")
                        status = result.get("status")
                        started_at = result.get("started_at")
                        completed_at = result.get("completed_at")
                        error_message = result.get("error_message")
                        
                        logger.info(f"Batch status check {check_count + 1}:")
                        logger.info(f"Status: {status}")
                        logger.info(f"Progress: {processed_docs}/{total_docs} processed")
                        logger.info(f"Failed: {failed_docs}")
                        
                        if status == "completed":
                            if failed_docs == 0:
                                logger.info(f"Batch completed successfully: {batch_id}")
                                logger.info(f"All {total_docs} documents processed")
                                logger.info(f"Completed at: {completed_at}")
                                return True
                            else:
                                logger.error(f"Batch completed with {failed_docs} failures: {batch_id}")
                                logger.error(f"Error: {error_message}")
                                return True
                                
                        elif status == "failed":
                            logger.error(f" Batch processing failed: {batch_id}")
                            logger.error(f"   Error: {error_message}")
                            return False
                            
                        elif status in ["processing", "pending"]:
                            logger.info(f"Batch still {status}, waiting...")
                            
                        else:
                            logger.warning(f"Unknown batch status: {status}")
                            
                    else:
                        error_text = response.text
                        logger.error(f"Status check error {response.status_code}: {error_text}")
                        
            except Exception as e:
                logger.error(f"Error checking batch status: {str(e)}")

            await asyncio.sleep(ScrapeDataController.STATUS_CHECK_INTERVAL)
        
        logger.error(f"Batch processing timeout after {ScrapeDataController.MAX_STATUS_CHECKS} checks: {batch_id}")
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
            if scraped_task_dir.exists():
                for subdir in scraped_task_dir.iterdir():
                    if subdir.is_dir():
                        try:
                            if not any(subdir.iterdir()):
                                subdir.rmdir()
                                logger.info(f"Removed empty scraped subdirectory: {subdir}")
                        except Exception as e:
                            logger.error(f"Error removing scraped subdirectory {subdir}: {str(e)}")

                if not any(scraped_task_dir.iterdir()):
                    scraped_task_dir.rmdir()
                    logger.info(f"Removed empty scraped task directory: {scraped_task_dir}")

            download_task_dir = base_path / "downloads" / task_id
            if download_task_dir.exists():
                for subdir in download_task_dir.iterdir():
                    if subdir.is_dir():
                        try:
                            if not any(subdir.iterdir()):
                                subdir.rmdir()
                                logger.info(f"Removed empty download subdirectory: {subdir}")
                        except Exception as e:
                            logger.error(f"Error removing download subdirectory {subdir}: {str(e)}")

                if not any(download_task_dir.iterdir()):
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
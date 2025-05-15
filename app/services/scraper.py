import json
import os
import re
import cloudscraper
from bs4 import BeautifulSoup
from markdownify import markdownify as md
import time
import hashlib
from urllib.parse import urlparse
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional, Tuple

class ClusterScraper:
    """
    A class to scrape content from clustered URLs and convert them to markdown.
    """
    
    def __init__(
        self,
        json_file_path: str,
        output_dir: str = "scraped_content",
        metadata_dir: str = "document_metadata",
        expiry_days: Optional[int] = None
    ):
        """
        Initialize the cluster scraper.

        Args:
            json_file_path: Path to the JSON file containing clusters
            output_dir: Base directory for saving scraped content
            metadata_dir: Directory for saving metadata files
            expiry_days: Number of days until document expiry
        """
        # Setup logger
        self.logger = self._setup_logger()
        
        # Store configuration
        self.json_file_path = json_file_path
        self.output_dir = output_dir
        self.metadata_dir = metadata_dir
        self.expiry_days = expiry_days
        self.clusters_data = self.load_clusters_json()
        
        # Status tracking
        self.status = "initialized"
        self.progress = 0.0
        self.start_time = 0.0
        self.pages_scraped = 0
        self.total_pages = 0
        self.current_cluster_id = ""
        self.error = None
    
    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("ClusterScraper")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def load_clusters_json(self) -> Optional[Dict[str, Any]]:
        """Load and parse the JSON file containing cluster information."""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading clusters JSON file: {e}")
            return None
    
    def get_cluster_by_id(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """
        Find a cluster by its ID in the nested JSON structure.

        Args:
            cluster_id: The ID of the cluster to find

        Returns:
            The cluster data if found, None otherwise
        """
        if not self.clusters_data:
            return None
        
        # Check top-level clusters first
        for domain, domain_data in self.clusters_data.get("clusters", {}).items():
            if domain_data.get("id") == cluster_id:
                return domain_data
            
            # Check nested clusters
            for sub_cluster in domain_data.get("clusters", []):
                if sub_cluster.get("id") == cluster_id:
                    return sub_cluster
        
        self.logger.warning(f"Cluster with ID '{cluster_id}' not found.")
        return None
    
    def get_scraper(self):
        """Create a scraper object using cloudscraper to bypass Cloudflare."""
        return cloudscraper.create_scraper()
    
    def fetch_page(self, url: str):
        """Fetch page content using cloudscraper."""
        scraper = self.get_scraper()
        response = scraper.get(url, allow_redirects=True)
        
        # Check if URL was redirected to a 404 page
        final_url = response.url
        if "/404" in final_url or (final_url != url and ("not-found" in final_url or "error" in final_url)):
            self.logger.warning(f"URL redirected to possible 404 page: {url} -> {final_url}")
            # Return a modified response with 404 status to signal it should be skipped
            response.status_code = 404
        
        return response
    
    def parse_and_convert_to_markdown(self, html: str) -> Tuple[str, str, str]:
        """
        Parse HTML content and convert to markdown, removing navigation, headers, footers, etc.
        
        Args:
            html: HTML content to parse and convert
            
        Returns:
            Tuple of (markdown_content, clean_filename, page_title)
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove headers, footers, navigation, and other non-content elements
        # for tag in soup.find_all(['header', 'footer', 'nav', 'aside', 'script', 'style', 'div'],
        #     class_=['col-lg-2 col-md-2 col-xs-2 col-sm-2 no-padding headerDiv','morph-menu-wrapper','phNumber',
        #         'show-on-hover topBarMenu portal-button','menu-premier-quick-links-container','mobile-nav',
        #         'quickContact paddingSidemenuDefault',
        #         'pum-container popmake theme-2087406 pum-responsive pum-responsive-small responsive size-small',
        #         'pum-container popmake theme-2087405 pum-responsive pum-responsive-medium responsive size-medium',
        #         'pum-content popmake-content',' col-sm-10 no-padding classForRes','fontEm13 normalFont marginTop0',
        #         'col-sm-10 no-padding','countrySelect clearfix','col-sm-2 no-padding',
        #         ' col-sm-10 no-padding classForRes'
        # ]):
        #     tag.decompose()

        for tag in soup.find_all(['header', 'footer', 'nav', 'aside', 'script', 'style', 'div'],
            class_=['mobile-login-field-small-wrapper','sub-page-links-wrapper','header-main-subpages','related-links-wrapper','content-wrapper','mobile-header-main', 'mm-header-nav-links','top-bar','login-field-small-wrapper-subpages','form-small-wrapper','side-nav-inner-page','footer-wrapper','mobile-copyrights-wrapper','privacy-links-wrapper','bread-crums-wrapper','dcp-form']):
            tag.decompose()
        
        # Find and remove all images
        for img in soup.find_all('img'):
            img.decompose()
        
        # Remove figure elements which might contain images
        for figure in soup.find_all('figure'):
            figure.decompose()
        
        # Remove picture elements (modern responsive image containers)
        for picture in soup.find_all('picture'):
            picture.decompose()
        
        # Remove forms
        # for form in soup.find_all('form'):
        #     form.decompose()
        
        # Remove SVG elements
        for svg in soup.find_all('svg'):
            svg.decompose()
        
        # Remove sections containing "Apply Now" headings
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            if heading.get_text(strip=True).lower() == "apply now":
                # Find the parent container
                parent_to_remove = None
                current = heading
                for _ in range(3):
                    if current.parent:
                        current = current.parent
                        if current.name in ['section', 'div', 'form']:
                            parent_to_remove = current
                            break
                
                if parent_to_remove:
                    parent_to_remove.decompose()
                else:
                    # If no suitable parent found, remove the heading and any following form
                    for elem in heading.find_next_siblings():
                        if elem.name == 'form' or ('form' in elem.get('class', '')):
                            elem.decompose()
                    heading.decompose()
        
        # Try to find main content
        # content = soup.find_all(['article', 'section', 'main', 'div', 'main id', 'p'], 
        #                       class_=['content', 'article-body', 'main-content', 'show', 
        #                              'main-heading', 'tab-content inner-txt-bx', 'container'])
        content = soup.find_all(['article', 'section', 'main', 'div', 'main id', 'p'], 
                                class_=['content', 'article-body', 'main-content',  'show', 'main-heading','tab-content inner-txt-bx','container'])
        
        if not content:
            content = soup.body
        
        if not content:
            self.logger.warning("No usable content found.")
            return "", "", ""
        
        # Get the page title
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            page_title = title_tag.string.strip()
        else:
            # Try to find an h1 tag
            h1_tag = soup.find('h1')
            if h1_tag:
                page_title = h1_tag.get_text(strip=True)
            else:
                page_title = "untitled"
        
        # Clean up the title to be a valid filename
        clean_title = re.sub(r'[^\w\s-]', '', page_title).strip()
        clean_title = re.sub(r'[-\s]+', '-', clean_title)
        
        # If title is still empty or just contains special characters that were removed
        if not clean_title:
            clean_title = "untitled-content"
        
        # Convert to Markdown
        markdown = md(str(content), heading_style="ATX")
        
        # Post-process the markdown to remove any remaining image markdown syntax
        markdown = re.sub(r'!\[.*?\]\(.*?\)', '', markdown)
        
        # Also remove any standalone image URLs that might remain
        markdown = re.sub(r'https?://\S+\.(jpg|jpeg|png|gif|svg|webp)(\?\S+)?', '', markdown, flags=re.IGNORECASE)
        
        return markdown, clean_title, page_title
    
    def determine_source_from_url(self, url: str) -> str:
        """
        Determine the source type based on the URL.
        
        Args:
            url: The URL to analyze
            
        Returns:
            Source type ('website', 'facebook', or 'manual')
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        if 'facebook.com' in domain or 'fb.com' in domain:
            return 'facebook'
        else:
            return 'website'
    
    def generate_document_id(self, content: str) -> str:
        """
        Generate a SHA-256 checksum for the document content.
        
        Args:
            content: The content to hash
            
        Returns:
            The SHA-256 hash
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def create_metadata_file(
        self, 
        metadata_dir: str, 
        filename_base: str, 
        document_name: str, 
        url: str, 
        content: str, 
        expiry: Optional[str] = None
    ) -> None:
        """
        Create a metadata file with document information.
        
        Args:
            metadata_dir: Directory to save the metadata file
            filename_base: Base filename (without extension)
            document_name: Name/title of the document
            url: URL of the document
            content: Content of the document for generating checksum
            expiry: Expiry date if set
        """
        os.makedirs(metadata_dir, exist_ok=True)
        
        source = self.determine_source_from_url(url)
        document_id = self.generate_document_id(content)
        
        metadata_content = f"document name: {document_name}\n"
        metadata_content += f"document url: {url}\n"
        metadata_content += f"expiry: {expiry if expiry else 'none'}\n"
        metadata_content += f"source: {source}\n"
        metadata_content += f"document_id: {document_id}\n"
        
        metadata_file_path = os.path.join(metadata_dir, f"{filename_base}.meta")
        with open(metadata_file_path, "w", encoding="utf-8") as f:
            f.write(metadata_content)
        
        self.logger.debug(f"Metadata saved to: {metadata_file_path}")
    
    def url_to_directory_path(self, url: str) -> str:
        """
        Convert a URL to a directory path structure.
        
        Args:
            url: URL to convert
            
        Returns:
            Directory path
        """
        # Parse URL to get path components
        parsed_url = url.split("://", 1)
        if len(parsed_url) < 2:
            return "unknown"
        
        domain_and_path = parsed_url[1].split("/", 1)
        domain = domain_and_path[0]
        
        # Create the base directory structure using domain
        dir_path = os.path.join(self.output_dir, domain)
        
        # Add path components if they exist
        if len(domain_and_path) > 1 and domain_and_path[1]:
            path_parts = domain_and_path[1].split("/")
            # Build full directory path
            for part in path_parts[:-1]:  # Exclude the last part as it might be a file
                if part:
                    dir_path = os.path.join(dir_path, part)
        
        return dir_path
    
    def scrape_clusters(self, cluster_ids: List[str]) -> Dict[str, Any]:
        """
        Scrape all URLs in the specified clusters.
        
        Args:
            cluster_ids: List of IDs of the clusters to scrape
            
        Returns:
            Dictionary with scraping results
        """
        import time
        self.start_time = time.time()
        self.status = "processing"
        self.progress = 0.0
        self.pages_scraped = 0
        self.error = None
        
        # Create metadata directory if it doesn't exist
        os.makedirs(self.metadata_dir, exist_ok=True)
        
        # Calculate expiry date if expiry_days is provided
        expiry_date = None
        if self.expiry_days is not None:
            expiry_date = (datetime.now() + timedelta(days=self.expiry_days)).strftime('%Y-%m-%d')
        
        # Count total URLs to scrape for progress tracking
        self.total_pages = 0
        for cluster_id in cluster_ids:
            cluster = self.get_cluster_by_id(cluster_id)
            if cluster:
                self.total_pages += len(cluster.get("urls", []))
        
        results = {
            "clusters_scraped": [],
            "pages_scraped": 0,
            "errors": []
        }
        
        for cluster_id in cluster_ids:
            self.current_cluster_id = cluster_id
            cluster = self.get_cluster_by_id(cluster_id)
            if not cluster:
                self.logger.warning(f"Skipping cluster {cluster_id} - not found")
                results["errors"].append({
                    "cluster_id": cluster_id,
                    "error": "Cluster not found"
                })
                continue
            
            urls = cluster.get("urls", [])
            
            if not urls:
                self.logger.warning(f"No URLs found in cluster {cluster_id}. Skipping.")
                continue
            
            self.logger.info(f"Found {len(urls)} URLs in cluster {cluster_id}. Starting scraping...")
            
            cluster_results = {
                "cluster_id": cluster_id,
                "urls_scraped": 0,
                "errors": []
            }
            
            for url in urls:
                try:
                    self.logger.info(f"Scraping: {url}")
                    
                    # Update progress
                    self.pages_scraped += 1
                    self.progress = min(99.0, (self.pages_scraped / self.total_pages) * 100)
                    
                    # Fetch the page
                    response = self.fetch_page(url)
                    
                    # Only process pages with 200 status code
                    if response.status_code != 200:
                        self.logger.warning(f"Skipping URL {url} - Status code: {response.status_code}")
                        cluster_results["errors"].append({
                            "url": url,
                            "error": f"Status code: {response.status_code}"
                        })
                        continue
                    
                    # Parse and convert to markdown
                    html = response.text
                    markdown_content, filename_base, document_title = self.parse_and_convert_to_markdown(html)
                    
                    if markdown_content:
                        # Create directory structure based on URL path
                        dir_path = self.url_to_directory_path(url)
                        os.makedirs(dir_path, exist_ok=True)
                        
                        # Save the markdown content
                        file_path = os.path.join(dir_path, f"{filename_base}.md")
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(markdown_content)
                        
                        self.logger.info(f"Content saved to: {file_path}")
                        
                        # Create the metadata file
                        self.create_metadata_file(
                            self.metadata_dir,
                            filename_base,
                            document_title,
                            url,
                            markdown_content,
                            expiry_date
                        )
                        
                        # Update counter
                        cluster_results["urls_scraped"] += 1
                    else:
                        self.logger.warning("No meaningful content extracted.")
                        cluster_results["errors"].append({
                            "url": url,
                            "error": "No meaningful content extracted"
                        })
                
                except Exception as e:
                    self.logger.error(f"Error processing {url}: {e}")
                    cluster_results["errors"].append({
                        "url": url,
                        "error": str(e)
                    })
                    
                    # Continue with the next URL
                    continue
            
            # Add cluster results
            results["clusters_scraped"].append(cluster_results)
            results["pages_scraped"] += cluster_results["urls_scraped"]
        
        # Update status
        self.status = "completed"
        self.progress = 100.0
        
        return results
    
    def list_available_clusters(self) -> List[Dict[str, Any]]:
        """
        List all available clusters with their IDs.
        
        Returns:
            List of dictionaries with cluster information
        """
        if not self.clusters_data:
            return []
        
        clusters_info = []
        
        # Go through domains
        for domain, domain_data in self.clusters_data.get("clusters", {}).items():
            # Add domain level clusters
            clusters_info.append({
                "id": domain_data.get("id"),
                "name": domain,
                "type": "domain",
                "url_count": domain_data.get("count", 0)
            })
            
            # Add sub-clusters
            for sub_cluster in domain_data.get("clusters", []):
                clusters_info.append({
                    "id": sub_cluster.get("id"),
                    "name": f"{domain} - {sub_cluster.get('path', 'unknown-path')}",
                    "type": "path",
                    "url_count": sub_cluster.get("url_count", 0)
                })
        
        return clusters_info
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the scraper."""
        import time
        return {
            'status': self.status,
            'progress': self.progress,
            'pages_scraped': self.pages_scraped,
            'total_pages': self.total_pages,
            'current_cluster': self.current_cluster_id,
            'execution_time_seconds': time.time() - self.start_time if self.start_time > 0 else 0,
            'error': self.error
        }
from typing import Dict, Any, Optional
from fastapi import HTTPException
from app.controllers.apollo_scrape.crawl_result_controller import CrawlResultController


class ClusterController:
    @staticmethod
    async def get_clusters(crawl_task_id: Optional[str] = None) -> Dict[str, Any]:
        response = {
            "clusters": [],
            "years": [],
            "clusters_available": False,
            "years_available": False
        }
        try:
            crawl_result = None
            
            if crawl_task_id:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
                if not crawl_result:
                    raise HTTPException(status_code=404, detail=f"Crawl result for task {crawl_task_id} not found")
            else:
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
            
            if not crawl_result:
                response["clusters_error"] = "No crawl results found in database"
                response["years_error"] = "No crawl results found in database"
                return response
            
            if crawl_result.clusters:
                response["clusters_available"] = True
                clusters_info = []
                
                for domain, domain_data in crawl_result.clusters.items():
                    clusters_info.append({
                        "id": domain_data.id, 
                        "name": domain,
                        "type": "domain",
                        "url_count": domain_data.count 
                    })
                    
                    for sub_cluster in domain_data.clusters:
                        clusters_info.append({
                            "id": sub_cluster.id,  
                            "name": f"{domain} - {sub_cluster.path}", 
                            "type": "path",
                            "url_count": sub_cluster.url_count  
                        })
                
                response["clusters"] = clusters_info
            else:
                response["clusters_error"] = f"No clusters found for crawl result {crawl_result.task_id}"
            
            if crawl_result.yearclusters:
                response["years_available"] = True
                years_info = []
                
                for year, files in crawl_result.yearclusters.items():
                    years_info.append({
                        "year": year,
                        "files_count": len(files)
                    })
                
                response["years"] = sorted(
                    years_info,
                    key=lambda y: (y["year"] == "No Year", y["year"]),
                    reverse=True
                )
            else:
                response["years_error"] = f"No years found for crawl result {crawl_result.task_id}"
            
            return response
            
        except HTTPException:
            raise
        except Exception as e:
            response["clusters_error"] = f"Error retrieving clusters: {str(e)}"
            response["years_error"] = f"Error retrieving years: {str(e)}"
            return response
    
    @staticmethod
    async def get_cluster_by_id(cluster_id: str, crawl_task_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            crawl_result = None
            
            if crawl_task_id:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
                if not crawl_result:
                    raise HTTPException(status_code=404, detail=f"Crawl result for task {crawl_task_id} not found")
            else:
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
            
            if not crawl_result or not crawl_result.clusters:
                raise HTTPException(status_code=404, detail="No crawl results with clusters found")

            for domain, domain_data in crawl_result.clusters.items():
                if domain_data.id == cluster_id:  
                    return {
                        "id": domain_data.id,
                        "name": domain,
                        "type": "domain",
                        "url_count": domain_data.count,
                        "clusters": [
                            {
                                "id": cluster.id,
                                "path": cluster.path,
                                "url_count": cluster.url_count,
                                "urls": cluster.urls
                            }
                            for cluster in domain_data.clusters
                        ]
                    }
                
                for cluster in domain_data.clusters:
                    if cluster.id == cluster_id:  
                        return {
                            "id": cluster.id,
                            "name": f"{domain} - {cluster.path}",
                            "type": "path",
                            "url_count": cluster.url_count,
                            "urls": cluster.urls
                        }
            
            raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error retrieving cluster: {str(e)}")

    @staticmethod
    async def get_year_by_id(year: str, crawl_task_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            crawl_result = None
            
            if crawl_task_id:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
                if not crawl_result:
                    raise HTTPException(status_code=404, detail=f"Crawl result for task {crawl_task_id} not found")
            else:
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
            
            if not crawl_result or not crawl_result.yearclusters:
                raise HTTPException(status_code=404, detail="No crawl results with year clusters found")
            
            if year in crawl_result.yearclusters:
                return {
                    "year": year,
                    "files_count": len(crawl_result.yearclusters[year]),
                    "files": crawl_result.yearclusters[year]
                }
            
            raise HTTPException(status_code=404, detail=f"Year {year} not found")
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error retrieving year: {str(e)}")
from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException, logger
from app.controllers.cluster_controller import ClusterController
from app.controllers.crawl_result_controller import CrawlResultController
from app.models.cluster_model import ClusterDetailResponse, YearDetailResponse

router = APIRouter(prefix="/api", tags=["Clusters"])

@router.get("/get-clusters", response_model=List[Dict[str, Any]])
async def get_crawl_results():
    try:
        crawl_results = await CrawlResultController.list_crawl_results()
        summary_results = []
        for result in crawl_results:
            clusters_data = {}
            cluster_summary = {
                "total_domains": 0,
                "total_clusters": 0,
                "total_urls": 0
            }
            
            if result.clusters:
                cluster_summary["total_domains"] = len(result.clusters)
                
                for domain_name, domain_data in result.clusters.items():
                    clusters_data[domain_name] = {
                        "id": domain_data.id,
                        "count": domain_data.count,
                        "clusters": []
                    }

                    for sub_cluster in domain_data.clusters:
                        clusters_data[domain_name]["clusters"].append({
                            "id": sub_cluster.id,
                            "path": sub_cluster.path,
                            "url_count": sub_cluster.url_count
                        })
                    
                    cluster_summary["total_clusters"] += len(domain_data.clusters)
                    cluster_summary["total_urls"] += domain_data.count
            
            years_data = {}
            year_summary = {
                "total_years": 0,
                "total_files": 0
            }
            
            if result.yearclusters:
                year_summary["total_years"] = len(result.yearclusters)
                for year, files in result.yearclusters.items():
                    file_count = len(files)
                    year_summary["total_files"] += file_count
                    years_data[year] = {
                        "year": year,
                        "files_count": file_count
                    }
            
            summary_results.append({
                "task_id": result.task_id,
                "link_found": result.link_found,
                "pages_scraped": result.pages_scraped,
                "is_scraped": result.is_scraped,
                "error": result.error,
                "created_at": result.created_at,
                "updated_at": result.updated_at,
                "clusters": clusters_data,
                "yearclusters": years_data,
                "cluster_summary": cluster_summary,
                "year_summary": year_summary
            })
        
        return summary_results
        
    except Exception as e:
        logger.error(f"Error getting crawl results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get crawl results: {str(e)}")

@router.get("/tasks/{task_id}/clusters/{cluster_id}", response_model=ClusterDetailResponse)
async def get_cluster_by_id(
    task_id: str,
    cluster_id: str
):
    return await ClusterController.get_cluster_by_id(cluster_id, task_id)

@router.get("/tasks/{task_id}/years/{year}", response_model=YearDetailResponse)
async def get_year_by_id(
    task_id: str,
    year: str
):
    return await ClusterController.get_year_by_id(year, task_id)
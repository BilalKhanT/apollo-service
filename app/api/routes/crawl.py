from typing import Optional
from fastapi import APIRouter, HTTPException, Query, status
from app.controllers.cluster_controller import ClusterController
from app.controllers.crawl_result_controller import CrawlResultController
from app.models.cluster_model import (
    ClusterDetailResponse, 
    YearDetailResponse,
    ClustersListResponse
)
from app.models.base import ErrorResponse, ListResponse
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["Clusters"])

@router.get(
    "/clusters",
    response_model=ClustersListResponse,
    responses={
        200: {
            "description": "Clusters and years retrieved successfully",
            "model": ClustersListResponse
        },
        404: {
            "description": "No crawl results found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error", 
            "model": ErrorResponse
        }
    },
    summary="Get available clusters and years",
    description="Retrieve all available clusters and years from crawl results for scraping and downloading."
)
async def get_clusters_and_years(
    crawl_task_id: Optional[str] = Query(
        None,
        description="Specific crawl task ID to get clusters from (uses most recent if not specified)",
        example="123e4567-e89b-12d3-a456-426614174000"
    )
) -> ClustersListResponse:
    try:
        result = await ClusterController.get_clusters(crawl_task_id=crawl_task_id)
        
        return ClustersListResponse(
            success=True,
            message="Clusters and years retrieved successfully",
            **result
        )
        
    except Exception as e:
        logger.error(f"Error getting clusters and years: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve clusters and years: {str(e)}"
        )

@router.get(
    "/get-clusters",
    response_model=ListResponse,
    responses={
        200: {
            "description": "Crawl results retrieved successfully",
            "model": ListResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Get all crawl results with summary information",
    description="Retrieve a list of all crawl results with cluster and year summaries."
)
async def get_crawl_results(
    limit: int = Query(50, ge=1, le=100, description="Number of results to return"),
    skip: int = Query(0, ge=0, description="Number of results to skip")
) -> ListResponse:
    try:
        crawl_results = await CrawlResultController.list_crawl_results()
        total_count = len(crawl_results)
        paginated_results = crawl_results[skip:skip + limit]
        summary_results = []
        
        # Karachi timezone offset (+5 hours)
        karachi_offset = timedelta(hours=5)
        
        for result in paginated_results:
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
            
            # Convert timestamps to Karachi time for response only
            created_at_karachi = result.created_at + karachi_offset if result.created_at else None
            updated_at_karachi = result.updated_at + karachi_offset if result.updated_at else None
            
            summary_results.append({
                "task_id": result.task_id,
                "link_found": result.link_found,
                "pages_scraped": result.pages_scraped,
                "is_scraped": result.is_scraped,
                "error": result.error,
                "created_at": created_at_karachi,
                "updated_at": updated_at_karachi,
                "clusters": clusters_data,
                "yearclusters": years_data,
                "cluster_summary": cluster_summary,
                "year_summary": year_summary
            })
        
        return ListResponse(
            success=True,
            message=f"Retrieved {len(summary_results)} crawl results",
            data=summary_results,
            total_count=total_count,
            page=skip // limit + 1,
            page_size=limit
        )
        
    except Exception as e:
        logger.error(f"Error getting crawl results: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get crawl results: {str(e)}"
        )

@router.get(
    "/tasks/{task_id}/clusters/{cluster_id}",
    response_model=ClusterDetailResponse,
    responses={
        200: {
            "description": "Cluster details retrieved successfully",
            "model": ClusterDetailResponse
        },
        404: {
            "description": "Task or cluster not found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Get detailed cluster information",
    description="Retrieve detailed information about a specific cluster including all URLs."
)
async def get_cluster_by_id(
    task_id: str,
    cluster_id: str
) -> ClusterDetailResponse:
    try:
        cluster_details = await ClusterController.get_cluster_by_id(cluster_id, task_id)
        return cluster_details
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cluster {cluster_id} for task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve cluster details: {str(e)}"
        )

@router.get(
    "/tasks/{task_id}/years/{year}",
    response_model=YearDetailResponse,
    responses={
        200: {
            "description": "Year details retrieved successfully",
            "model": YearDetailResponse
        },
        404: {
            "description": "Task or year not found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Get detailed year information",
    description="Retrieve detailed information about files available for a specific year."
)
async def get_year_by_id(
    task_id: str,
    year: str
) -> YearDetailResponse:
    try:
        year_details = await ClusterController.get_year_by_id(year, task_id)
        return year_details
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting year {year} for task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve year details: {str(e)}"
        )
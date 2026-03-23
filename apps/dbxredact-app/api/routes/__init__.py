"""API router aggregation."""

from fastapi import APIRouter
from .config import router as config_router
from .pipeline import router as pipeline_router
from .review import router as review_router
from .metrics import router as metrics_router
from .lists import router as lists_router
from .labels import router as labels_router
from .ab_test import router as ab_test_router
from .active_learn import router as active_learn_router
from .benchmark import router as benchmark_router
from .catalog import router as catalog_router
from .admin import router as admin_router

api_router = APIRouter()
api_router.include_router(config_router, prefix="/config", tags=["config"])
api_router.include_router(pipeline_router, prefix="/pipeline", tags=["pipeline"])
api_router.include_router(benchmark_router, prefix="/benchmark", tags=["benchmark"])
api_router.include_router(review_router, prefix="/review", tags=["review"])
api_router.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
api_router.include_router(lists_router, prefix="/lists", tags=["lists"])
api_router.include_router(labels_router, prefix="/labels", tags=["labels"])
api_router.include_router(ab_test_router, prefix="/ab-tests", tags=["ab-tests"])
api_router.include_router(active_learn_router, prefix="/active-learn", tags=["active-learn"])
api_router.include_router(catalog_router, prefix="/catalog", tags=["catalog"])
api_router.include_router(admin_router, prefix="/admin", tags=["admin"])

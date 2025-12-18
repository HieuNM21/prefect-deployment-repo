
import json
from typing import Any, Dict
from prefect import flow, task, get_run_logger

@flow(name="TEST_FLOW_01")
def pipeline_runner(pipeline: Dict[str, Any]):
    logger = get_run_logger()
    logger.info(f"Running pipeline: {pipeline.get('name')}")

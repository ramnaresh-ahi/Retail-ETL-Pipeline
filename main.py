# main.py - Complete ETL Pipeline Orchestrator with Basic Validation

import logging
import sys
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Import ETL modules
try:
    from src.extract import extract_retail_data
    from src.transform import transform_retail_data
    from src.load import load_to_postgres
except ImportError as e:
    print(f"❌ Failed to import ETL modules: {e}")
    sys.exit(1)

def setup_logging(level: str = "INFO") -> logging.Logger:
    """Initialize logging to file and console."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"etl_{datetime.today():%Y%m%d}.log"
    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=level, format=fmt,
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler()])
    logger = logging.getLogger()
    logger.info(f"Logging initialized at {level}, file: {log_file}")
    return logger

logger = setup_logging()

class ETLPipelineError(Exception):
    """Custom exception for ETL pipeline errors."""
    pass

class ETLPipelineOrchestrator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.results: Dict[str, Any] = {
            "pipeline_id": f"etl_{datetime.now():%Y%m%d_%H%M%S}",
            "pipeline_start": None,
            "pipeline_end": None,
            "extract_results": None,
            "transform_results": None,
            "load_results": None,
            "success": False,
            "errors": []
        }

    def validate_extracted_data(self, extract_results: Dict[str, Any]) -> Dict[str, Any]:
        """Basic validation: file exists & readable."""
        logger.info("Validating extracted data...")
        vr = extract_results.get("validation_results", {})
        info = vr.get("sales.csv", {})
        exists = info.get("exists", False)
        readable = info.get("valid", False)
        logger.info(f"File exists: {exists}, readable: {readable}")
        return {"exists": exists, "readable": readable, "all_passed": exists and readable}

    def run(self) -> Dict[str, Any]:
        """Execute Extract → Transform → Load with basic validation."""
        self.results["pipeline_start"] = datetime.now()
        logger.info("Starting ETL pipeline...")

        # Extract
        extract_start = time.time()
        extract_results = extract_retail_data(force_download=self.config.get("force_extract", False))
        self.results["extract_results"] = extract_results
        if not extract_results.get("success"):
            raise ETLPipelineError("Extract phase failed")
        vext = self.validate_extracted_data(extract_results)
        if not vext["all_passed"]:
            raise ETLPipelineError("Extract validation failed")
        logger.info("Extract phase succeeded")

        # Transform
        transform_start = time.time()
        transform_results = transform_retail_data()
        if transform_results is None:
            raise ETLPipelineError("Transform phase failed")
        self.results["transform_results"] = transform_results
        logger.info("Transform phase succeeded")

        # Load
        load_start = time.time()
        load_results = load_to_postgres()
        if not load_results:
            raise ETLPipelineError("Load phase failed")
        self.results["load_results"] = load_results
        logger.info("Load phase succeeded")

        self.results["pipeline_end"] = datetime.now()
        self.results["success"] = True
        return self.results

def main():
    # Parse flags
    force_extract = "--force-extract" in sys.argv

    orchestrator = ETLPipelineOrchestrator({"force_extract": force_extract})
    try:
        results = orchestrator.run()
        duration = (results["pipeline_end"] - results["pipeline_start"]).total_seconds()
        logger.info(f"ETL pipeline completed in {duration:.2f}s")
        print(json.dumps({"success": True, "duration": duration}, indent=2))
        sys.exit(0)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        print(json.dumps({"success": False, "error": str(e)}))
        sys.exit(1)

if __name__ == "__main__":
    main()

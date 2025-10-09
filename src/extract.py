# src/extract.py - Advanced Data Extraction with Monitoring & Validation
import os
import logging
import hashlib
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
import polars as pl

# Set up comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BASE_DIR = Path(__file__).resolve().parent.parent
DOTENV_PATH = BASE_DIR / ".env"
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
METADATA_DIR = BASE_DIR / "data" / "metadata"

# Dataset configuration
DATASET_CONFIG = {
    'dataset_slug': 'ytgangster/online-sales-in-usa',
    'expected_files': ['sales.csv'],  # Files we expect to find
    'min_file_size_mb': 1,  # Minimum expected file size
    'max_file_age_days': 30,  # Maximum age before re-download
}

class DataExtractionError(Exception):
    """Custom exception for data extraction errors"""
    pass

def load_environment_variables() -> Dict[str, str]:
    """Load and validate environment variables"""
    logger.info("🔑 Loading environment variables...")
    
    # Load environment variables
    load_dotenv(dotenv_path=DOTENV_PATH)
    
    # Get Kaggle credentials
    username = os.getenv("KAGGLE_USERNAME")
    key = os.getenv("KAGGLE_KEY")
    
    if not username or not key:
        raise DataExtractionError(
            "Kaggle credentials not found in .env file!\n"
            "Please add KAGGLE_USERNAME and KAGGLE_KEY to your .env file"
        )
    
    # Set credentials for Kaggle API
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = key
    
    logger.info(f"✅ Loaded credentials for user: {username}")
    
    # ✅ FIXED: Import kaggle AFTER setting environment variables
    try:
        global kaggle
        import kaggle
        logger.info("✅ Kaggle library imported successfully")
    except Exception as e:
        raise DataExtractionError(f"Failed to import Kaggle library: {e}")
    
    return {
        'username': username,
        'key': key[:8] + "..." + key[-4:] if len(key) > 12 else "***"  # Masked for security
    }

def setup_directories() -> Dict[str, Path]:
    """Create necessary directories and return paths"""
    logger.info("📁 Setting up directory structure...")
    
    directories = {
        'raw_data': RAW_DATA_DIR,
        'metadata': METADATA_DIR,
        'backup': BASE_DIR / "data" / "backup"
    }
    
    for name, path in directories.items():
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"✅ Directory ready: {name} -> {path}")
    
    return directories

def check_existing_data() -> Dict[str, any]:
    """Check if data already exists and validate it"""
    logger.info("🔍 Checking for existing data...")
    
    existing_files = {}
    
    for expected_file in DATASET_CONFIG['expected_files']:
        file_path = RAW_DATA_DIR / expected_file
        
        if file_path.exists():
            # Get file stats
            stat = file_path.stat()
            size_mb = stat.st_size / (1024 * 1024)
            age_days = (datetime.now().timestamp() - stat.st_mtime) / (24 * 3600)
            
            # Calculate file hash for integrity
            file_hash = calculate_file_hash(file_path)
            
            existing_files[expected_file] = {
                'path': file_path,
                'size_mb': round(size_mb, 2),
                'age_days': round(age_days, 2),
                'hash': file_hash,
                'last_modified': datetime.fromtimestamp(stat.st_mtime),
                'valid_size': size_mb >= DATASET_CONFIG['min_file_size_mb'],
                'recent': age_days <= DATASET_CONFIG['max_file_age_days']
            }
            
            logger.info(f"📊 Found {expected_file}:")
            logger.info(f"   Size: {size_mb:.2f} MB")
            logger.info(f"   Age: {age_days:.2f} days")
            logger.info(f"   Hash: {file_hash[:16]}...")
        else:
            existing_files[expected_file] = None
            logger.info(f"❌ Missing: {expected_file}")
    
    return existing_files

def calculate_file_hash(file_path: Path) -> str:
    """Calculate SHA256 hash of a file for integrity checking"""
    sha256_hash = hashlib.sha256()
    
    with open(file_path, "rb") as f:
        # Read file in chunks to handle large files
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()

def backup_existing_data(existing_files: Dict[str, any]) -> None:
    """Backup existing data files before downloading new ones"""
    logger.info("💾 Backing up existing data files...")
    
    backup_dir = BASE_DIR / "data" / "backup"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for file_name, file_info in existing_files.items():
        if file_info:  # File exists
            source_path = file_info['path']
            backup_name = f"{file_name.replace('.csv', '')}_{timestamp}.csv"
            backup_path = backup_dir / backup_name
            
            try:
                # Copy file to backup
                import shutil
                shutil.copy2(source_path, backup_path)
                logger.info(f"✅ Backed up {file_name} -> {backup_name}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to backup {file_name}: {e}")

def test_kaggle_connection() -> bool:
    """Test Kaggle API authentication and basic functionality."""
    logger.info("🔌 Testing Kaggle API connection...")
    try:
        # Authenticate (will raise if credentials invalid)
        kaggle.api.authenticate()

        # Simple list call without unsupported args
        kaggle.api.dataset_list(search="test", page=1)
        logger.info("✅ Kaggle API connection successful")
        return True

    except TypeError:
        # Fallback if dataset_list signature changed
        try:
            kaggle.api.authenticate()
            logger.info("✅ Kaggle API authenticated (no list test)")
            return True
        except Exception as e:
            logger.error(f"❌ Kaggle API authentication failed: {e}")
            return False

    except Exception as e:
        logger.error(f"❌ Kaggle API connection failed: {e}")
        return False

def download_dataset_with_progress(dataset_slug: str, download_path: Path) -> Dict[str, any]:
    """Download dataset with progress monitoring"""
    logger.info(f"📦 Starting download: {dataset_slug}")
    start_time = time.time()
    
    try:
        # Download dataset
        kaggle.api.dataset_download_files(
            dataset_slug,
            path=str(download_path),
            unzip=True
        )
        
        download_time = time.time() - start_time
        logger.info(f"✅ Download completed in {download_time:.2f} seconds")
        
        return {
            'success': True,
            'download_time': download_time,
            'timestamp': datetime.now(),
            'dataset_slug': dataset_slug
        }
        
    except Exception as e:
        logger.error(f"❌ Dataset download failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'download_time': time.time() - start_time,
            'timestamp': datetime.now(),
            'dataset_slug': dataset_slug
        }

def validate_downloaded_data() -> Dict[str, any]:
    """Validate downloaded data files"""
    logger.info("🔍 Validating downloaded data...")
    
    validation_results = {}
    
    for expected_file in DATASET_CONFIG['expected_files']:
        file_path = RAW_DATA_DIR / expected_file
        
        if not file_path.exists():
            validation_results[expected_file] = {
                'exists': False,
                'error': 'File not found after download'
            }
            continue
        
        try:
            # Basic file validation
            stat = file_path.stat()
            size_mb = stat.st_size / (1024 * 1024)
            
            # Try to read file with Polars to check format
            df = pl.read_csv(file_path, n_rows=10)  # Read only first 10 rows for validation
            
            validation_results[expected_file] = {
                'exists': True,
                'size_mb': round(size_mb, 2),
                'rows_sample': df.height,
                'columns': df.width,
                'column_names': df.columns,
                'file_hash': calculate_file_hash(file_path),
                'valid': size_mb >= DATASET_CONFIG['min_file_size_mb'] and df.height > 0
            }
            
            logger.info(f"✅ {expected_file} validation:")
            logger.info(f"   Size: {size_mb:.2f} MB")
            logger.info(f"   Columns: {df.width}")
            logger.info(f"   Sample rows: {df.height}")
            
        except Exception as e:
            validation_results[expected_file] = {
                'exists': True,
                'valid': False,
                'error': str(e)
            }
            logger.error(f"❌ Validation failed for {expected_file}: {e}")
    
    return validation_results

def save_extraction_metadata(download_info: Dict, validation_results: Dict) -> None:
    """Save extraction metadata for tracking"""
    logger.info("💾 Saving extraction metadata...")
    
    metadata = {
        'extraction_timestamp': datetime.now().isoformat(),
        'dataset_slug': DATASET_CONFIG['dataset_slug'],
        'download_info': download_info,
        'validation_results': validation_results,
        'files_extracted': list(validation_results.keys()),
        'extraction_success': download_info.get('success', False) and 
                            all(result.get('valid', False) for result in validation_results.values())
    }
    
    # Save metadata as JSON
    import json
    metadata_file = METADATA_DIR / f"extraction_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    logger.info(f"✅ Metadata saved to: {metadata_file}")

def generate_extraction_report(existing_files: Dict, download_info: Dict, validation_results: Dict) -> str:
    """Generate comprehensive extraction report"""
    
    total_files = len(DATASET_CONFIG['expected_files'])
    valid_files = sum(1 for result in validation_results.values() if result.get('valid', False))
    total_size_mb = sum(result.get('size_mb', 0) for result in validation_results.values())
    
    report = f"""
📋 DATA EXTRACTION REPORT
{'='*50}
Dataset: {DATASET_CONFIG['dataset_slug']}
Extraction Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 SUMMARY:
- Files Expected: {total_files}
- Files Downloaded: {len(validation_results)}
- Files Valid: {valid_files}
- Total Size: {total_size_mb:.2f} MB
- Download Time: {download_info.get('download_time', 0):.2f} seconds
- Success Rate: {(valid_files/total_files)*100:.1f}%

📁 FILE DETAILS:"""

    for file_name, result in validation_results.items():
        if result.get('valid', False):
            report += f"""
✅ {file_name}:
   - Size: {result['size_mb']:.2f} MB
   - Columns: {result.get('columns', 'N/A')}
   - Hash: {result.get('file_hash', 'N/A')[:16]}..."""
        else:
            report += f"""
❌ {file_name}:
   - Status: FAILED
   - Error: {result.get('error', 'Unknown error')}"""

    return report

def extract_retail_data(force_download: bool = False) -> Dict[str, any]:
    """Main data extraction function"""
    
    logger.info("🚀 Starting data extraction pipeline...")
    start_time = datetime.now()
    
    try:
        # 1. Load environment and setup (this imports kaggle safely)
        credentials = load_environment_variables()
        directories = setup_directories()
        
        # 2. Check existing data
        existing_files = check_existing_data()
        
        # 3. Determine if download is needed
        need_download = force_download
        
        if not force_download:
            for file_name, file_info in existing_files.items():
                if not file_info:  # File doesn't exist
                    need_download = True
                    break
                elif not file_info.get('valid_size') or not file_info.get('recent'):
                    need_download = True
                    logger.info(f"📅 {file_name} is outdated or too small, will re-download")
        
        if not need_download:
            logger.info("✅ All data files are recent and valid, skipping download")
            validation_results = {}
            for file_name, file_info in existing_files.items():
                if file_info:
                    validation_results[file_name] = {
                        'exists': True,
                        'size_mb': file_info['size_mb'],
                        'valid': True,
                        'skipped': True
                    }
            
            return {
                'success': True,
                'skipped_download': True,
                'validation_results': validation_results,
                'processing_time': (datetime.now() - start_time).total_seconds()
            }
        
        # 4. Test Kaggle connection
        if not test_kaggle_connection():
            raise DataExtractionError("Kaggle API connection failed")
        
        # 5. Backup existing data
        backup_existing_data(existing_files)
        
        # 6. Download dataset
        download_info = download_dataset_with_progress(
            DATASET_CONFIG['dataset_slug'],
            RAW_DATA_DIR
        )
        
        if not download_info['success']:
            raise DataExtractionError(f"Download failed: {download_info['error']}")
        
        # 7. Validate downloaded data
        validation_results = validate_downloaded_data()
        
        # 8. Save metadata
        save_extraction_metadata(download_info, validation_results)
        
        # 9. Generate report
        report = generate_extraction_report(existing_files, download_info, validation_results)
        logger.info(report)
        
        # 10. Final results
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        success = all(result.get('valid', False) for result in validation_results.values())
        
        logger.info(f"✅ Data extraction completed successfully!")
        logger.info(f"⏱️  Total processing time: {processing_time:.2f} seconds")
        
        return {
            'success': success,
            'download_info': download_info,
            'validation_results': validation_results,
            'processing_time': processing_time,
            'files_extracted': list(validation_results.keys())
        }
        
    except Exception as e:
        logger.error(f"❌ Data extraction failed: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'processing_time': (datetime.now() - start_time).total_seconds()
        }

# Main execution
if __name__ == "__main__":
    print("=" * 60)
    print("🏪 RETAIL ETL PIPELINE - DATA EXTRACTION")
    print("=" * 60)
    
    # Add command line argument support
    import sys
    force_download = "--force" in sys.argv
    
    if force_download:
        print("🔄 Force download mode enabled")
    
    try:
        # Run extraction
        results = extract_retail_data(force_download=force_download)
        
        print("\n📋 EXTRACTION SUMMARY:")
        print("=" * 50)
        
        if results['success']:
            if results.get('skipped_download'):
                print("✅ SUCCESS: Using existing valid data files")
            else:
                print("✅ SUCCESS: Data extraction completed!")
                print(f"📦 Files extracted: {len(results['files_extracted'])}")
            
            print(f"⏱️  Processing time: {results['processing_time']:.2f} seconds")
            
            if results.get('validation_results'):
                for file_name, result in results['validation_results'].items():
                    if result.get('valid'):
                        status = "✅ VALID" if not result.get('skipped') else "✅ VALID (EXISTING)"
                        print(f"{file_name}: {status} - {result.get('size_mb', 0):.2f} MB")
        else:
            print("❌ EXTRACTION FAILED!")
            print(f"Error: {results.get('error', 'Unknown error')}")
        
        print(f"\n📁 Data location: {RAW_DATA_DIR}")
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {str(e)}")
        print("Please check the error logs above for details.")

#!/usr/bin/env python3
"""
Example Usage - B3DataExtractor with Local and S3 Storage

This script demonstrates how to use the updated B3DataExtractor
with both local and S3 storage options.
"""

import os
import logging
from ETL.extract import B3DataExtractor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def example_local_storage():
    """Example using local storage"""
    logger.info("=== Example: Local Storage ===")
    
    # Create extractor for local storage
    extractor = B3DataExtractor(
        storage_type="local",
        local_root="extracted_raw",  # This will create extracted_raw folder
        page_size=100,  # Smaller page size for testing
        index="IBOV",
        language="pt-br"
    )
    
    # Extract data
    raw_data = extractor.extract_data()
    if not raw_data:
        logger.error("Failed to extract data")
        return False
    
    # Prepare DataFrame
    df = extractor.prepare_dataframe(raw_data)
    if df.empty:
        logger.error("Empty DataFrame after processing")
        return False
    
    # Save to local storage
    success = extractor.save_data(df)
    
    if success:
        logger.info("✅ Data saved locally successfully!")
        logger.info("📁 Check the 'extracted_raw' folder for the parquet files")
        return True
    else:
        logger.error("❌ Failed to save data locally")
        return False

def example_s3_storage():
    """Example using S3 storage"""
    logger.info("=== Example: S3 Storage ===")
    
    # Check if S3_BUCKET is configured
    s3_bucket = os.getenv('S3_BUCKET')
    if not s3_bucket or s3_bucket == 'your-bucket-name-here':
        logger.error("❌ S3_BUCKET environment variable not configured")
        logger.info("💡 Set S3_BUCKET environment variable to use S3 storage")
        return False
    
    # Create extractor for S3 storage
    extractor = B3DataExtractor(
        storage_type="s3",
        s3_bucket=s3_bucket,
        page_size=100,  # Smaller page size for testing
        index="IBOV",
        language="pt-br",
        aws_region="us-east-1"
    )
    
    # Extract data
    raw_data = extractor.extract_data()
    if not raw_data:
        logger.error("Failed to extract data")
        return False
    
    # Prepare DataFrame
    df = extractor.prepare_dataframe(raw_data)
    if df.empty:
        logger.error("Empty DataFrame after processing")
        return False
    
    # Save to S3
    success = extractor.save_data(df)
    
    if success:
        logger.info("✅ Data saved to S3 successfully!")
        return True
    else:
        logger.error("❌ Failed to save data to S3")
        return False

def main():
    """Main function to demonstrate both storage types"""
    logger.info("🚀 B3DataExtractor Example Usage")
    logger.info("This script demonstrates local and S3 storage options")
    
    # Example 1: Local Storage
    local_success = example_local_storage()
    
    # Example 2: S3 Storage (only if configured)
    s3_success = example_s3_storage()
    
    # Summary
    logger.info("=== Summary ===")
    logger.info(f"Local storage: {'✅ Success' if local_success else '❌ Failed'}")
    logger.info(f"S3 storage: {'✅ Success' if s3_success else '❌ Not configured or failed'}")
    
    if local_success:
        logger.info("📁 Local files are saved in the 'extracted_raw' folder")
        logger.info("📊 Files follow the same partitioning structure as S3:")
        logger.info("   extracted_raw/year=YYYY/month=MM/day=DD/ibovespa_YYYYMMDD.parquet")

if __name__ == "__main__":
    main() 
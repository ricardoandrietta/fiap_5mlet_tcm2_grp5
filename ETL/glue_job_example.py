#!/usr/bin/env python3
"""
AWS Glue Job Example - B3 IBOVESPA ETL

This script demonstrates how to use the B3DataExtractor class in AWS Glue.
It shows the structure without the main() function wrapper.

Usage in AWS Glue:
1. Upload this script and ETL/ folder to S3
2. Create Glue job pointing to this script
3. Configure job parameters (see below)
4. Run the job

Glue Job Parameters:
- --S3_BUCKET: Target S3 bucket for output
- --B3_PAGE_SIZE: Records per page (default: 1200)
- --B3_INDEX: B3 index (default: IBOV)
- --B3_LANGUAGE: API language (default: pt-br)
- --B3_EXTRACT_ALL_PAGES: Extract all pages (default: false)
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

# Import our ETL module
from ETL.extract import B3DataExtractor

# Setup logging for Glue
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get Glue job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET',
    'B3_PAGE_SIZE',
    'B3_INDEX', 
    'B3_LANGUAGE',
    'B3_EXTRACT_ALL_PAGES'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def run_b3_etl():
    """
    Execute B3 ETL process in AWS Glue environment
    """
    try:
        logger.info("=== Starting B3 IBOVESPA ETL in AWS Glue ===")
        
        # Parse parameters
        s3_bucket = args['S3_BUCKET']
        page_size = int(args.get('B3_PAGE_SIZE', '1200'))
        index = args.get('B3_INDEX', 'IBOV')
        language = args.get('B3_LANGUAGE', 'pt-br')
        extract_all_pages = args.get('B3_EXTRACT_ALL_PAGES', 'false').lower() == 'true'
        
        logger.info(f"Configuration:")
        logger.info(f"  S3 Bucket: {s3_bucket}")
        logger.info(f"  Page Size: {page_size}")
        logger.info(f"  Index: {index}")
        logger.info(f"  Language: {language}")
        logger.info(f"  Extract All Pages: {extract_all_pages}")
        
        # Create extractor instance
        extractor = B3DataExtractor(
            s3_bucket=s3_bucket,
            page_size=page_size,
            index=index,
            language=language,
            aws_region='us-east-1'  # Default region, can be parameterized
        )
        
        # Extract data
        if extract_all_pages:
            logger.info("Extracting all available pages...")
            raw_data = extractor.extract_all_pages()
        else:
            logger.info("Extracting first page only...")
            raw_data = extractor.extract_data()
        
        if not raw_data:
            raise Exception("Failed to extract data from B3 API")
        
        # Prepare DataFrame
        df = extractor.prepare_dataframe(raw_data)
        
        if df.empty:
            raise Exception("Empty DataFrame after processing")
        
        logger.info(f"Processed {len(df)} records")
        
        # Save to S3
        success = extractor.save_to_s3(df)
        
        if not success:
            raise Exception("Failed to save data to S3")
        
        logger.info("=== B3 ETL completed successfully ===")
        
        # Optional: Create Glue Data Catalog table
        # create_glue_catalog_table(spark, s3_bucket)
        
        return True
        
    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        raise e

def create_glue_catalog_table(spark, s3_bucket):
    """
    Optional: Create/update Glue Data Catalog table for the extracted data
    
    Args:
        spark: Spark session
        s3_bucket: S3 bucket name
    """
    try:
        logger.info("Creating/updating Glue Data Catalog table...")
        
        # Read the saved parquet files
        s3_path = f"s3://{s3_bucket}/ibovespa-data/"
        
        # Create external table in Glue Data Catalog
        df = spark.read.parquet(s3_path)
        
        df.write \
          .mode("overwrite") \
          .option("path", s3_path) \
          .saveAsTable("b3_database.ibovespa_daily")
        
        logger.info("Glue Data Catalog table created/updated successfully")
        
    except Exception as e:
        logger.warning(f"Failed to create Glue catalog table: {e}")

# Execute the ETL process
if __name__ == "__main__":
    try:
        run_b3_etl()
        job.commit()
    except Exception as e:
        logger.error(f"Glue job failed: {e}")
        sys.exit(1) 
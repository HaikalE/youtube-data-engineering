#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilities for interacting with AWS S3.
"""

import os
import boto3
import pandas as pd
import json
import logging
from io import StringIO, BytesIO
from typing import Dict, List, Optional, Union, Any
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class S3Handler:
    """
    Class to handle S3 operations for the YouTube trending analysis.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize S3 handler with configuration.
        
        Args:
            config (Dict): Configuration dictionary from config.yaml
        """
        self.region_name = config['aws']['region_name']
        self.bucket_name = config['aws']['s3_bucket_name']
        self.raw_data_prefix = config['aws']['raw_data_prefix']
        self.processed_data_prefix = config['aws']['processed_data_prefix']
        self.analysis_prefix = config['aws']['analysis_prefix']
        self.dashboard_prefix = config['aws']['dashboard_prefix']
        
        # AWS credentials from environment variables
        self.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
        if not (self.aws_access_key_id and self.aws_secret_access_key):
            logger.warning("AWS credentials not found in environment variables. "
                          "Will attempt to use instance profile or AWS CLI configuration.")
        
        self.s3_client = self._get_s3_client()
        self.s3_resource = self._get_s3_resource()
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
    
    def _get_s3_client(self):
        """
        Create S3 client.
        
        Returns:
            boto3.client.S3: S3 client
        """
        try:
            if self.aws_access_key_id and self.aws_secret_access_key:
                return boto3.client(
                    's3',
                    region_name=self.region_name,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key
                )
            else:
                return boto3.client('s3', region_name=self.region_name)
        except Exception as e:
            logger.error(f"Error creating S3 client: {str(e)}")
            raise
    
    def _get_s3_resource(self):
        """
        Create S3 resource.
        
        Returns:
            boto3.resource.S3: S3 resource
        """
        try:
            if self.aws_access_key_id and self.aws_secret_access_key:
                return boto3.resource(
                    's3',
                    region_name=self.region_name,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key
                )
            else:
                return boto3.resource('s3', region_name=self.region_name)
        except Exception as e:
            logger.error(f"Error creating S3 resource: {str(e)}")
            raise
    
    def _ensure_bucket_exists(self):
        """
        Check if bucket exists, create it if it doesn't.
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} exists.")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Bucket {self.bucket_name} does not exist. Creating...")
                try:
                    if self.region_name == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region_name}
                        )
                    logger.info(f"Bucket {self.bucket_name} created successfully.")
                except Exception as create_error:
                    logger.error(f"Error creating bucket: {str(create_error)}")
                    raise
            else:
                logger.error(f"Error checking bucket existence: {str(e)}")
                raise
    
    def upload_dataframe_to_csv(self, df: pd.DataFrame, prefix: str, filename: str) -> str:
        """
        Upload a DataFrame to S3 as a CSV file.
        
        Args:
            df (pd.DataFrame): DataFrame to upload
            prefix (str): S3 prefix (folder)
            filename (str): Filename (without extension)
            
        Returns:
            str: S3 URI of the uploaded file
        """
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            s3_key = f"{prefix}{filename}.csv"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue()
            )
            
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Successfully uploaded DataFrame to {s3_uri}")
            
            return s3_uri
        except Exception as e:
            logger.error(f"Error uploading DataFrame to S3: {str(e)}")
            raise
    
    def upload_dataframe_to_parquet(self, df: pd.DataFrame, prefix: str, filename: str) -> str:
        """
        Upload a DataFrame to S3 as a Parquet file.
        
        Args:
            df (pd.DataFrame): DataFrame to upload
            prefix (str): S3 prefix (folder)
            filename (str): Filename (without extension)
            
        Returns:
            str: S3 URI of the uploaded file
        """
        try:
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            parquet_buffer.seek(0)
            
            s3_key = f"{prefix}{filename}.parquet"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
            
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Successfully uploaded DataFrame to {s3_uri}")
            
            return s3_uri
        except Exception as e:
            logger.error(f"Error uploading DataFrame to S3: {str(e)}")
            raise
    
    def upload_raw_data(self, category_dfs: Dict[int, pd.DataFrame], timestamp: str) -> Dict[int, str]:
        """
        Upload raw trending data to S3.
        
        Args:
            category_dfs (Dict[int, pd.DataFrame]): Dictionary of raw DataFrames by category
            timestamp (str): Timestamp to use in the filename
            
        Returns:
            Dict[int, str]: Dictionary mapping category IDs to S3 URIs
        """
        s3_uris = {}
        
        for cat_id, df in category_dfs.items():
            try:
                filename = f"trending_raw_cat_{cat_id}_{timestamp}"
                s3_uri = self.upload_dataframe_to_parquet(df, self.raw_data_prefix, filename)
                s3_uris[cat_id] = s3_uri
            except Exception as e:
                logger.error(f"Error uploading raw data for category {cat_id}: {str(e)}")
                # Continue with other categories even if one fails
                continue
        
        return s3_uris
    
    def upload_processed_data(self, category_dfs: Dict[int, pd.DataFrame], timestamp: str) -> Dict[int, str]:
        """
        Upload processed trending data to S3.
        
        Args:
            category_dfs (Dict[int, pd.DataFrame]): Dictionary of processed DataFrames by category
            timestamp (str): Timestamp to use in the filename
            
        Returns:
            Dict[int, str]: Dictionary mapping category IDs to S3 URIs
        """
        s3_uris = {}
        
        for cat_id, df in category_dfs.items():
            try:
                filename = f"trending_processed_cat_{cat_id}_{timestamp}"
                s3_uri = self.upload_dataframe_to_parquet(df, self.processed_data_prefix, filename)
                s3_uris[cat_id] = s3_uri
            except Exception as e:
                logger.error
                logger.error(f"Error uploading processed data for category {cat_id}: {str(e)}")
                # Continue with other categories even if one fails
                continue
        
        return s3_uris
    
    def upload_analysis_results(self, analysis_results: Dict, timestamp: str) -> str:
        """
        Upload analysis results to S3.
        
        Args:
            analysis_results (Dict): Dictionary containing analysis results
            timestamp (str): Timestamp to use in the filename
            
        Returns:
            str: S3 URI of the uploaded file
        """
        try:
            s3_key = f"{self.analysis_prefix}analysis_results_{timestamp}.json"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(analysis_results, default=str),
                ContentType='application/json'
            )
            
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Successfully uploaded analysis results to {s3_uri}")
            
            return s3_uri
        except Exception as e:
            logger.error(f"Error uploading analysis results to S3: {str(e)}")
            raise
    
    def upload_dashboard_assets(self, filename: str, content: Union[str, bytes], content_type: str = None) -> str:
        """
        Upload dashboard assets like HTML, CSS, JS files to S3.
        
        Args:
            filename (str): Filename including extension
            content (Union[str, bytes]): Content to upload
            content_type (str, optional): Content type MIME. Default is None (auto-detect).
            
        Returns:
            str: S3 URI of the uploaded file
        """
        try:
            s3_key = f"{self.dashboard_prefix}{filename}"
            
            # Convert string to bytes if needed
            if isinstance(content, str):
                content = content.encode('utf-8')
            
            # Auto-detect content type if not provided
            if content_type is None:
                if filename.endswith('.html'):
                    content_type = 'text/html'
                elif filename.endswith('.css'):
                    content_type = 'text/css'
                elif filename.endswith('.js'):
                    content_type = 'application/javascript'
                elif filename.endswith('.json'):
                    content_type = 'application/json'
                elif filename.endswith('.png'):
                    content_type = 'image/png'
                elif filename.endswith('.jpg') or filename.endswith('.jpeg'):
                    content_type = 'image/jpeg'
                else:
                    content_type = 'application/octet-stream'
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type
            )
            
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Successfully uploaded dashboard asset to {s3_uri}")
            
            return s3_uri
        except Exception as e:
            logger.error(f"Error uploading dashboard asset to S3: {str(e)}")
            raise
    
    def download_dataframe_from_parquet(self, s3_uri: str) -> pd.DataFrame:
        """
        Download a parquet file from S3 and load it as a DataFrame.
        
        Args:
            s3_uri (str): S3 URI of the parquet file
            
        Returns:
            pd.DataFrame: DataFrame loaded from parquet file
        """
        try:
            # Parse S3 URI
            if not s3_uri.startswith('s3://'):
                raise ValueError(f"Invalid S3 URI: {s3_uri}")
            
            parts = s3_uri[5:].split('/', 1)
            bucket = parts[0]
            key = parts[1]
            
            buffer = BytesIO()
            self.s3_client.download_fileobj(bucket, key, buffer)
            buffer.seek(0)
            
            df = pd.read_parquet(buffer)
            logger.info(f"Successfully downloaded DataFrame from {s3_uri}")
            
            return df
        except Exception as e:
            logger.error(f"Error downloading DataFrame from S3: {str(e)}")
            raise
    
    def list_files(self, prefix: str) -> List[str]:
        """
        List files in an S3 prefix.
        
        Args:
            prefix (str): S3 prefix to list
            
        Returns:
            List[str]: List of S3 URIs
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            s3_uris = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    s3_uris.append(f"s3://{self.bucket_name}/{obj['Key']}")
            
            return s3_uris
        except Exception as e:
            logger.error(f"Error listing files in S3: {str(e)}")
            raise
    
    def get_latest_processed_data(self, category_id: Optional[int] = None) -> Dict[int, pd.DataFrame]:
        """
        Get the latest processed data for all categories or a specific category.
        
        Args:
            category_id (Optional[int]): Category ID to get. Default is None (all categories).
            
        Returns:
            Dict[int, pd.DataFrame]: Dictionary mapping category IDs to DataFrames
        """
        try:
            prefix = self.processed_data_prefix
            all_files = self.list_files(prefix)
            
            # Filter files by category if needed
            if category_id is not None:
                all_files = [f for f in all_files if f"_cat_{category_id}_" in f]
            
            # Group files by category
            category_files = {}
            for file_uri in all_files:
                # Extract category from filename
                match = re.search(r"_cat_(\d+)_", file_uri)
                if match:
                    cat_id = int(match.group(1))
                    if cat_id not in category_files:
                        category_files[cat_id] = []
                    category_files[cat_id].append(file_uri)
            
            # Get the latest file for each category
            latest_dfs = {}
            for cat_id, files in category_files.items():
                # Sort files by timestamp (newest first)
                files.sort(reverse=True)
                if files:  # If there are files for this category
                    latest_df = self.download_dataframe_from_parquet(files[0])
                    latest_dfs[cat_id] = latest_df
            
            return latest_dfs
        except Exception as e:
            logger.error(f"Error getting latest processed data: {str(e)}")
            raise

def main(config_path: str):
    """
    Main function to test S3 utilities.
    
    Args:
        config_path (str): Path to the configuration file
    """
    import yaml
    
    # Load configuration
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Create S3 handler
    s3_handler = S3Handler(config)
    
    # List files in raw data prefix
    raw_files = s3_handler.list_files(s3_handler.raw_data_prefix)
    print(f"Raw files: {raw_files}")
    
    # List files in processed data prefix
    processed_files = s3_handler.list_files(s3_handler.processed_data_prefix)
    print(f"Processed files: {processed_files}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "../config/config.yaml"
    
    main(config_path)
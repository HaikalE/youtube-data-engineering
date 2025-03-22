#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Load module for storing processed data in S3 and database.
"""

import os
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Optional, Union

from utils.s3_utils import S3Handler
from utils.db_utils import DatabaseHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YouTubeLoader:
    """
    Class to load YouTube data into storage systems.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the YouTube Loader with configuration.
        
        Args:
            config (Dict): Configuration dictionary from config.yaml
        """
        self.config = config
        self.s3_handler = S3Handler(config)
        self.db_handler = DatabaseHandler(config)
        
        # Create database tables if they don't exist
        self.db_handler.create_tables()
    
    def load_to_s3(self, raw_data: Dict[int, pd.DataFrame], processed_data: Dict[int, pd.DataFrame]) -> Dict:
        """
        Load raw and processed data to S3.
        
        Args:
            raw_data (Dict[int, pd.DataFrame]): Dictionary of raw DataFrames by category
            processed_data (Dict[int, pd.DataFrame]): Dictionary of processed DataFrames by category
            
        Returns:
            Dict: Dictionary with S3 URIs for uploaded files
        """
        logger.info("Uploading data to S3...")
        
        # Generate timestamp for filenames
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # Upload raw data
        raw_uris = self.s3_handler.upload_raw_data(raw_data, timestamp)
        logger.info(f"Uploaded {len(raw_uris)} raw data files to S3")
        
        # Upload processed data
        processed_uris = self.s3_handler.upload_processed_data(processed_data, timestamp)
        logger.info(f"Uploaded {len(processed_uris)} processed data files to S3")
        
        return {
            'timestamp': timestamp,
            'raw_uris': raw_uris,
            'processed_uris': processed_uris
        }
    
    def load_to_database(self, processed_data: Dict[int, pd.DataFrame]) -> str:
        """
        Load processed data to database.
        
        Args:
            processed_data (Dict[int, pd.DataFrame]): Dictionary of processed DataFrames by category
            
        Returns:
            str: Batch ID used for the database load
        """
        logger.info("Loading data to database...")
        
        # Use the batch_id from the first DataFrame (should be the same for all)
        if not processed_data:
            raise ValueError("No processed data to load to database")
        
        first_df = next(iter(processed_data.values()))
        batch_id = first_df['batch_id'].iloc[0]
        
        # Combine all DataFrames
        combined_df = pd.concat(processed_data.values(), ignore_index=True)
        
        # Store in database
        self.db_handler.store_trending_videos(combined_df)
        
        # Calculate and store aggregated statistics
        self.db_handler.calculate_channel_stats(batch_id)
        self.db_handler.calculate_trends_summary(batch_id)
        self.db_handler.calculate_hashtag_stats(batch_id)
        
        logger.info(f"Successfully loaded data to database with batch_id: {batch_id}")
        
        return batch_id
    
    def load_data(self, raw_data: Dict[int, pd.DataFrame], processed_data: Dict[int, pd.DataFrame]) -> Dict:
        """
        Load data to all storage systems.
        
        Args:
            raw_data (Dict[int, pd.DataFrame]): Dictionary of raw DataFrames by category
            processed_data (Dict[int, pd.DataFrame]): Dictionary of processed DataFrames by category
            
        Returns:
            Dict: Dictionary with loading results
        """
        # Load to S3
        s3_results = self.load_to_s3(raw_data, processed_data)
        
        # Load to database
        batch_id = self.load_to_database(processed_data)
        
        return {
            'timestamp': s3_results['timestamp'],
            'batch_id': batch_id,
            's3_uris': {
                'raw': s3_results['raw_uris'],
                'processed': s3_results['processed_uris']
            }
        }

def main(config_path: str, raw_data: Dict[int, pd.DataFrame], processed_data: Dict[int, pd.DataFrame]) -> Dict:
    """
    Main function to load YouTube data.
    
    Args:
        config_path (str): Path to the configuration file
        raw_data (Dict[int, pd.DataFrame]): Dictionary of raw DataFrames by category
        processed_data (Dict[int, pd.DataFrame]): Dictionary of processed DataFrames by category
    
    Returns:
        Dict: Dictionary with loading results
    """
    import yaml
    
    # Load configuration
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Load data
    loader = YouTubeLoader(config)
    results = loader.load_data(raw_data, processed_data)
    
    return results

if __name__ == "__main__":
    import sys
    import pickle
    
    if len(sys.argv) > 3:
        config_path = sys.argv[1]
        raw_data_path = sys.argv[2]
        processed_data_path = sys.argv[3]
        
        # Load data from pickle files
        with open(raw_data_path, 'rb') as f:
            raw_data = pickle.load(f)
        
        with open(processed_data_path, 'rb') as f:
            processed_data = pickle.load(f)
        
        # Load data
        results = main(config_path, raw_data, processed_data)
        
        # If output path is provided, save the results
        if len(sys.argv) > 4:
            output_path = sys.argv[4]
            with open(output_path, 'wb') as f:
                pickle.dump(results, f)
            print(f"Results saved to {output_path}")
        else:
            print("Results:", results)
    else:
        print("Usage: python loader.py <config_path> <raw_data_path> <processed_data_path> [output_path]")
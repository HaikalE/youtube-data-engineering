#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Transform module for processing and enhancing YouTube data.
"""

import pandas as pd
import numpy as np
import re
import json
import logging
import os
import pickle
from typing import Dict, List, Union
from datetime import datetime
import isodate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YouTubeTransformer:
    """
    Class to transform and enrich YouTube trending data.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the YouTube Transformer with configuration.
        
        Args:
            config (Dict): Configuration dictionary from config.yaml
        """
        self.config = config
    
    def parse_duration(self, duration: str) -> int:
        """
        Convert ISO 8601 duration to seconds.
        
        Args:
            duration (str): ISO 8601 duration string
            
        Returns:
            int: Duration in seconds
        """
        try:
            return int(isodate.parse_duration(duration).total_seconds())
        except Exception as e:
            logger.warning(f"Could not parse duration {duration}: {str(e)}")
            return 0
    
    def extract_hashtags(self, text: str) -> List[str]:
        """
        Extract hashtags from a string.
        
        Args:
            text (str): String containing hashtags
            
        Returns:
            List[str]: List of hashtags
        """
        if not text:
            return []
        
        # Find all hashtags in the text
        hashtags = re.findall(r'#(\w+)', text)
        return hashtags
    
    def calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived metrics from basic video stats.
        
        Args:
            df (pd.DataFrame): DataFrame with video data
            
        Returns:
            pd.DataFrame: DataFrame with additional metrics
        """
        # Make a copy to avoid modifying the original
        df_transformed = df.copy()
        
        # Convert publish_time and extracted_at to datetime and ensure both are timezone-aware
        # Force both to be UTC to avoid timezone comparison issues
        try:
            # First convert to datetime
            df_transformed['publish_time'] = pd.to_datetime(df_transformed['publish_time'])
            df_transformed['extracted_at'] = pd.to_datetime(df_transformed['extracted_at'])
            
            # Handle timezone - make sure both are naive (no timezone)
            if df_transformed['publish_time'].dt.tz is not None:
                df_transformed['publish_time'] = df_transformed['publish_time'].dt.tz_localize(None)
            
            if df_transformed['extracted_at'].dt.tz is not None:
                df_transformed['extracted_at'] = df_transformed['extracted_at'].dt.tz_localize(None)
            
            # Calculate how long the video has been published before extraction
            df_transformed['hours_since_published'] = (
                df_transformed['extracted_at'] - df_transformed['publish_time']
            ).dt.total_seconds() / 3600
            
        except Exception as e:
            logger.error(f"Error processing datetime fields: {str(e)}")
            # If datetime processing fails, add a default value for hours_since_published
            df_transformed['hours_since_published'] = 24  # Default to 24 hours
        
        # Calculate engagement metrics (only if view_count > 0)
        df_transformed['like_view_ratio'] = np.where(
            df_transformed['view_count'] > 0,
            df_transformed['like_count'] / df_transformed['view_count'] * 100,
            0
        )
        
        df_transformed['comment_view_ratio'] = np.where(
            df_transformed['view_count'] > 0,
            df_transformed['comment_count'] / df_transformed['view_count'] * 100,
            0
        )
        
        # Calculate views per hour (only if hours_since_published > 0)
        df_transformed['views_per_hour'] = np.where(
            df_transformed['hours_since_published'] > 0,
            df_transformed['view_count'] / df_transformed['hours_since_published'],
            df_transformed['view_count']  # If hours_since_published is 0, just use view_count
        )
        
        # Parse the duration
        df_transformed['duration_seconds'] = df_transformed['duration'].apply(self.parse_duration)
        
        # Create video length categories
        df_transformed['length_category'] = pd.cut(
            df_transformed['duration_seconds'],
            bins=[0, 60, 300, 600, 1200, float('inf')],
            labels=['< 1 min', '1-5 min', '5-10 min', '10-20 min', '> 20 min']
        )
        
        return df_transformed
    
    def extract_text_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract features from text fields like title and description.
        
        Args:
            df (pd.DataFrame): DataFrame with video data
            
        Returns:
            pd.DataFrame: DataFrame with additional text features
        """
        df_with_features = df.copy()
        
        # Extract hashtags from title and description
        df_with_features['title_hashtags'] = df_with_features['title'].apply(self.extract_hashtags)
        
        # Handle missing description
        df_with_features['description'] = df_with_features['description'].fillna("")
        df_with_features['description_hashtags'] = df_with_features['description'].apply(self.extract_hashtags)
        
        # Combine all hashtags
        df_with_features['all_hashtags'] = df_with_features.apply(
            lambda row: row['title_hashtags'] + row['description_hashtags'], axis=1
        )
        
        # Convert tags from JSON string to list
        df_with_features['tags_list'] = df_with_features['tags'].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x else []
        )
        
        # Title metrics
        df_with_features['title_length'] = df_with_features['title'].apply(len)
        df_with_features['title_word_count'] = df_with_features['title'].apply(lambda x: len(str(x).split()))
        
        # Description metrics
        df_with_features['has_description'] = df_with_features['description'].apply(lambda x: len(str(x)) > 0)
        df_with_features['description_length'] = df_with_features['description'].apply(lambda x: len(str(x)))
        
        return df_with_features
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations to the data.
        
        Args:
            df (pd.DataFrame): Raw trending videos DataFrame
            
        Returns:
            pd.DataFrame: Transformed DataFrame with additional features
        """
        logger.info(f"Transforming data with {len(df)} records")
        
        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return df
        
        try:
            # Apply transformations
            df_metrics = self.calculate_derived_metrics(df)
            df_transformed = self.extract_text_features(df_metrics)
            
            # Generate a unique file identifier (extraction timestamp)
            try:
                extraction_ts = pd.to_datetime(df['extracted_at'].iloc[0]).strftime('%Y%m%d%H%M%S')
            except:
                extraction_ts = datetime.now().strftime('%Y%m%d%H%M%S')
                
            df_transformed['batch_id'] = extraction_ts
            
            # Reorder columns for better readability
            column_order = [
                'batch_id', 'video_id', 'title', 'channel_id', 'channel_title', 
                'category_id', 'category_name', 'publish_time', 'extracted_at',
                'view_count', 'like_count', 'comment_count', 'duration', 'duration_seconds',
                'length_category', 'hours_since_published', 'views_per_hour',
                'like_view_ratio', 'comment_view_ratio'
            ]
            
            # Add the remaining columns
            remaining_columns = [col for col in df_transformed.columns if col not in column_order]
            column_order.extend(remaining_columns)
            
            # Make sure all columns in column_order exist in df_transformed
            available_columns = [col for col in column_order if col in df_transformed.columns]
            df_transformed = df_transformed[available_columns]
            
            logger.info(f"Transformation completed successfully")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def transform_all_categories(self, category_dfs: Dict[int, pd.DataFrame]) -> Dict[int, pd.DataFrame]:
        """
        Transform data for all categories.
        
        Args:
            category_dfs (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            
        Returns:
            Dict[int, pd.DataFrame]: Dictionary of transformed DataFrames by category
        """
        transformed_dfs = {}
        
        for cat_id, df in category_dfs.items():
            try:
                transformed_df = self.transform_data(df)
                if not transformed_df.empty:
                    transformed_dfs[cat_id] = transformed_df
                    logger.info(f"Transformation completed successfully for category {cat_id}")
                else:
                    logger.warning(f"Transformation resulted in empty DataFrame for category {cat_id}")
            except Exception as e:
                logger.error(f"Error transforming data for category {cat_id}: {str(e)}")
                # Continue with other categories even if one fails
                continue
        
        logger.info(f"Transformed {len(transformed_dfs)}/{len(category_dfs)} categories successfully")
        return transformed_dfs

def main(config_path: str, input_path: str = None) -> Dict[int, pd.DataFrame]:
    """
    Main function to transform YouTube trending data.
    
    Args:
        config_path (str): Path to the configuration file
        input_path (str): Path to the input data file
    
    Returns:
        Dict[int, pd.DataFrame]: Dictionary of transformed DataFrames by category
    """
    import yaml
    
    # Load configuration
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # If input path is not provided, use default
    if input_path is None:
        input_path = 'data/raw_data.pkl'
    
    # Load input data
    try:
        with open(input_path, 'rb') as f:
            input_data = pickle.load(f)
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_path}")
        raise
    
    # Transform data
    transformer = YouTubeTransformer(config)
    transformed_data = transformer.transform_all_categories(input_data)
    
    # Save transformed data
    os.makedirs('data', exist_ok=True)
    output_path = 'data/processed_data.pkl'
    with open(output_path, 'wb') as f:
        pickle.dump(transformed_data, f)
    
    logger.info(f"Saved processed data to {output_path}")
    return transformed_data

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
        
        # If input path is provided
        if len(sys.argv) > 2:
            input_data_path = sys.argv[2]
            transformed_data = main(config_path, input_data_path)
        else:
            transformed_data = main(config_path)
            
        # If output path is provided, save the results
        if len(sys.argv) > 3:
            output_path = sys.argv[3]
            with open(output_path, 'wb') as f:
                pickle.dump(transformed_data, f)
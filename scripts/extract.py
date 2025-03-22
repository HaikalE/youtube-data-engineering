#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Extract module responsible for getting data from YouTube API.
"""

import os
import json
import pandas as pd
import pickle
from datetime import datetime
import googleapiclient.discovery
import googleapiclient.errors
import logging
from typing import Dict, List, Optional, Union
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YouTubeExtractor:
    """
    Class to extract data from YouTube API.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the YouTube Extractor with configuration.
        
        Args:
            config (Dict): Configuration dictionary from config.yaml
        """
        self.api_service_name = config['youtube_api']['api_service_name']
        self.api_version = config['youtube_api']['api_version']
        self.region_code = config['youtube_api']['region_code']
        self.max_results = config['youtube_api']['max_results']
        self.categories = {int(cat['id']): cat['name'] for cat in config['youtube_api']['categories']}
        self.api_key = os.environ.get("YOUTUBE_API_KEY")
        self.alternative_regions = ["GB", "CA", "AU", "IN", "FR", "DE", "JP", "KR", "BR", "RU"]
        
        if not self.api_key:
            raise ValueError("YOUTUBE_API_KEY environment variable not set")
        
        self.youtube = self._get_youtube_client()
        
    def _get_youtube_client(self):
        """
        Create YouTube API client.
        
        Returns:
            googleapiclient.discovery.Resource: YouTube API client
        """
        try:
            return googleapiclient.discovery.build(
                self.api_service_name, 
                self.api_version, 
                developerKey=self.api_key
            )
        except Exception as e:
            logger.error(f"Error creating YouTube client: {str(e)}")
            raise
    
    def get_trending_videos(self, category_id: Optional[int] = None, region_code: Optional[str] = None) -> pd.DataFrame:
        """
        Get trending videos from YouTube API.
        
        Args:
            category_id (Optional[int]): YouTube category ID. Default is None (all categories).
            region_code (Optional[str]): Region code to use. Default is None (use configured region).
            
        Returns:
            pd.DataFrame: DataFrame containing trending videos data
        """
        region = region_code if region_code else self.region_code
        logger.info(f"Fetching trending videos for region: {region}, "
                   f"category: {self.categories.get(category_id, 'All')}")
        
        try:
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics",
                chart="mostPopular",
                regionCode=region,
                maxResults=self.max_results,
                videoCategoryId=str(category_id) if category_id else ""
            )
            
            response = request.execute()
            
            # Process the response into a dataframe
            videos = []
            for item in response.get("items", []):
                video_data = {
                    "video_id": item["id"],
                    "title": item["snippet"]["title"],
                    "channel_id": item["snippet"]["channelId"],
                    "channel_title": item["snippet"]["channelTitle"],
                    "publish_time": item["snippet"]["publishedAt"],
                    "description": item["snippet"].get("description", ""),
                    "tags": json.dumps(item["snippet"].get("tags", [])),
                    "category_id": item["snippet"]["categoryId"],
                    "category_name": self.categories.get(int(item["snippet"]["categoryId"]), "Unknown"),
                    "thumbnail_url": item["snippet"]["thumbnails"]["high"]["url"],
                    "duration": item["contentDetails"]["duration"],
                    "view_count": int(item["statistics"].get("viewCount", 0)),
                    "like_count": int(item["statistics"].get("likeCount", 0)),
                    "comment_count": int(item["statistics"].get("commentCount", 0)),
                    "extracted_at": datetime.now().isoformat()
                }
                videos.append(video_data)
            
            df = pd.DataFrame(videos)
            logger.info(f"Successfully extracted {len(df)} trending videos")
            return df
            
        except googleapiclient.errors.HttpError as e:
            logger.error(f"HTTP error occurred for region {region}: {str(e)}")
            if "videoChartNotFound" in str(e):
                logger.warning(f"Trending chart not available for region: {region}")
                return pd.DataFrame()  # Return empty DataFrame for this region
            raise
        except Exception as e:
            logger.error(f"Error fetching trending videos: {str(e)}")
            raise
    
    def try_multiple_regions(self, category_id: Optional[int] = None) -> pd.DataFrame:
        """
        Try to get trending videos from multiple regions if primary region fails.
        
        Args:
            category_id (Optional[int]): YouTube category ID. Default is None.
            
        Returns:
            pd.DataFrame: DataFrame containing trending videos data
        """
        # First try the configured region
        df = self.get_trending_videos(category_id)
        
        # If no results, try alternative regions
        if df.empty:
            logger.info(f"No results for primary region {self.region_code}, trying alternatives...")
            for region in self.alternative_regions:
                try:
                    df = self.get_trending_videos(category_id, region)
                    if not df.empty:
                        logger.info(f"Successfully retrieved data from region: {region}")
                        return df
                    # Add a small delay to avoid rate limiting
                    time.sleep(1)
                except Exception as e:
                    logger.warning(f"Failed to get data from region {region}: {str(e)}")
                    continue
        
        return df
    
    def get_videos_by_category(self) -> Dict[int, pd.DataFrame]:
        """
        Get trending videos for each category.
        
        Returns:
            Dict[int, pd.DataFrame]: Dictionary mapping category IDs to DataFrames
        """
        category_dfs = {}
        
        # First get all trending videos
        all_trending = self.try_multiple_regions()
        if not all_trending.empty:
            category_dfs[0] = all_trending
            logger.info(f"Successfully retrieved {len(all_trending)} trending videos")
        else:
            logger.warning("Could not retrieve trending videos for any region")
            # Create a sample dataset for testing if no real data is available
            if os.environ.get("GENERATE_SAMPLE_DATA", "false").lower() == "true":
                category_dfs[0] = self._generate_sample_data()
        
        # Then get trending videos for each category
        for cat_id in [cat_id for cat_id in self.categories.keys() if cat_id != 0]:
            try:
                cat_df = self.try_multiple_regions(cat_id)
                if not cat_df.empty:
                    category_dfs[cat_id] = cat_df
                else:
                    logger.warning(f"No data available for category {cat_id}")
            except Exception as e:
                logger.warning(f"Error fetching trending videos for category {cat_id}: {str(e)}")
                # Continue with other categories even if one fails
                continue
        
        return category_dfs
    
    def _generate_sample_data(self) -> pd.DataFrame:
        """
        Generate sample data for testing when API fails.
        
        Returns:
            pd.DataFrame: Sample trending videos data
        """
        logger.info("Generating sample data for testing")
        
        # Create sample data with realistic values
        sample_data = []
        categories = list(self.categories.items())
        
        for i in range(50):  # Generate 50 sample videos
            cat_id, cat_name = categories[i % len(categories)]
            sample_data.append({
                "video_id": f"sample_id_{i}",
                "title": f"Sample Video Title {i}",
                "channel_id": f"channel_{i % 10}",
                "channel_title": f"Sample Channel {i % 10}",
                "publish_time": (datetime.now().replace(hour=i % 24)).isoformat(),
                "description": f"This is a sample description for video {i}",
                "tags": json.dumps([f"tag{j}" for j in range(5)]),
                "category_id": str(cat_id),
                "category_name": cat_name,
                "thumbnail_url": f"https://example.com/thumbnail_{i}.jpg",
                "duration": f"PT{(i % 30) + 1}M",
                "view_count": 10000 + (i * 1000),
                "like_count": 1000 + (i * 100),
                "comment_count": 100 + (i * 10),
                "extracted_at": datetime.now().isoformat()
            })
        
        return pd.DataFrame(sample_data)

def main(config_path: str):
    """
    Main function to extract YouTube trending data.
    
    Args:
        config_path (str): Path to the configuration file
    
    Returns:
        Dict[int, pd.DataFrame]: Dictionary of DataFrames by category
    """
    import yaml
    
    # Load configuration
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Extract data
    extractor = YouTubeExtractor(config)
    category_dfs = extractor.get_videos_by_category()
    
    # Check if data was retrieved
    if all(df.empty for df in category_dfs.values()):
        logger.error("Failed to retrieve any data from YouTube API")
        if "GENERATE_SAMPLE_DATA" not in os.environ:
            logger.info("You can set GENERATE_SAMPLE_DATA=true to use sample data for testing")
        return {}
    
    # Save the data to file
    os.makedirs('data', exist_ok=True)  # Ensure data directory exists
    output_path = 'data/raw_data.pkl'
    with open(output_path, 'wb') as f:
        pickle.dump(category_dfs, f)
    
    logger.info(f"Saved raw data to {output_path}")
    return category_dfs

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "../config/config.yaml"
    
    main(config_path)
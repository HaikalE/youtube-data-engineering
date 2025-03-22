#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Extract module responsible for getting data from YouTube API.
"""

import os
import json
import pandas as pd
from datetime import datetime
import googleapiclient.discovery
import googleapiclient.errors
import logging
from typing import Dict, List, Optional, Union

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
    
    def get_trending_videos(self, category_id: Optional[int] = None) -> pd.DataFrame:
        """
        Get trending videos from YouTube API.
        
        Args:
            category_id (Optional[int]): YouTube category ID. Default is None (all categories).
            
        Returns:
            pd.DataFrame: DataFrame containing trending videos data
        """
        logger.info(f"Fetching trending videos for region: {self.region_code}, "
                   f"category: {self.categories.get(category_id, 'All')}")
        
        try:
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics",
                chart="mostPopular",
                regionCode=self.region_code,
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
            logger.error(f"HTTP error occurred: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error fetching trending videos: {str(e)}")
            raise
    
    def get_videos_by_category(self) -> Dict[int, pd.DataFrame]:
        """
        Get trending videos for each category.
        
        Returns:
            Dict[int, pd.DataFrame]: Dictionary mapping category IDs to DataFrames
        """
        category_dfs = {}
        
        # First get all trending videos
        all_trending = self.get_trending_videos()
        category_dfs[0] = all_trending
        
        # Then get trending videos for each category
        for cat_id in [cat_id for cat_id in self.categories.keys() if cat_id != 0]:
            try:
                category_dfs[cat_id] = self.get_trending_videos(cat_id)
            except Exception as e:
                logger.warning(f"Error fetching trending videos for category {cat_id}: {str(e)}")
                # Continue with other categories even if one fails
                continue
        
        return category_dfs

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
    
    return category_dfs

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "../config/config.yaml"
    
    main(config_path)
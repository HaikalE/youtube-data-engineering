#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Analysis module for YouTube trending data.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import logging
from typing import Dict, List, Optional, Union, Any
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

from utils.s3_utils import S3Handler
from utils.db_utils import DatabaseHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YouTubeAnalyzer:
    """
    Class to analyze YouTube trending data.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the YouTube Analyzer with configuration.
        
        Args:
            config (Dict): Configuration dictionary from config.yaml
        """
        self.config = config
        self.s3_handler = S3Handler(config)
        self.db_handler = DatabaseHandler(config)
    
    def analyze_category_trends(self, data: Dict[int, pd.DataFrame]) -> Dict:
        """
        Analyze trends by category.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            
        Returns:
            Dict: Category trends analysis
        """
        logger.info("Analyzing category trends...")
        
        # Combine all DataFrames for overall analysis
        combined_df = pd.concat(data.values(), ignore_index=True)
        
        # Group by category
        category_stats = combined_df.groupby(['category_id', 'category_name']).agg({
            'video_id': 'count',
            'view_count': 'mean',
            'like_count': 'mean',
            'comment_count': 'mean',
            'duration_seconds': 'mean',
            'like_view_ratio': 'mean',
            'comment_view_ratio': 'mean',
            'views_per_hour': 'mean'
        }).reset_index()
        
        # Rename columns for clarity
        category_stats = category_stats.rename(columns={
            'video_id': 'video_count',
            'view_count': 'avg_views',
            'like_count': 'avg_likes',
            'comment_count': 'avg_comments',
            'duration_seconds': 'avg_duration',
            'like_view_ratio': 'avg_like_view_ratio',
            'comment_view_ratio': 'avg_comment_view_ratio',
            'views_per_hour': 'avg_views_per_hour'
        })
        
        # Sort by video count (descending)
        category_stats = category_stats.sort_values('video_count', ascending=False)
        
        # Convert to JSON-serializable format
        return json.loads(category_stats.to_json(orient='records'))
    
    def analyze_top_videos(self, data: Dict[int, pd.DataFrame], limit: int = 20) -> Dict:
        """
        Identify top trending videos.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            limit (int): Maximum number of videos to return
            
        Returns:
            Dict: Top videos analysis
        """
        logger.info(f"Analyzing top {limit} videos...")
        
        # Combine all DataFrames
        combined_df = pd.concat(data.values(), ignore_index=True)
        
        # Get top videos by views per hour
        top_by_views_per_hour = combined_df.sort_values('views_per_hour', ascending=False).head(limit)
        top_by_views_per_hour = top_by_views_per_hour[[
            'video_id', 'title', 'channel_title', 'category_name', 
            'view_count', 'views_per_hour', 'like_count', 'comment_count',
            'duration_seconds', 'publish_time'
        ]]
        
        # Get top videos by like-to-view ratio (minimum 10000 views)
        top_by_likes = combined_df[combined_df['view_count'] >= 10000]
        top_by_likes = top_by_likes.sort_values('like_view_ratio', ascending=False).head(limit)
        top_by_likes = top_by_likes[[
            'video_id', 'title', 'channel_title', 'category_name', 
            'view_count', 'like_view_ratio', 'like_count', 'comment_count',
            'duration_seconds', 'publish_time'
        ]]
        
        # Convert to JSON-serializable format
        return {
            'top_by_views_per_hour': json.loads(top_by_views_per_hour.to_json(orient='records')),
            'top_by_likes': json.loads(top_by_likes.to_json(orient='records'))
        }
    
    def analyze_top_channels(self, data: Dict[int, pd.DataFrame], limit: int = 20) -> Dict:
        """
        Identify top channels in trending videos.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            limit (int): Maximum number of channels to return
            
        Returns:
            Dict: Top channels analysis
        """
        logger.info(f"Analyzing top {limit} channels...")
        
        # Combine all DataFrames
        combined_df = pd.concat(data.values(), ignore_index=True)
        
        # Group by channel
        channel_stats = combined_df.groupby(['channel_id', 'channel_title']).agg({
            'video_id': 'count',
            'view_count': 'mean',
            'like_count': 'mean',
            'comment_count': 'mean',
            'like_view_ratio': 'mean',
            'comment_view_ratio': 'mean',
            'views_per_hour': 'mean'
        }).reset_index()
        
        # Rename columns for clarity
        channel_stats = channel_stats.rename(columns={
            'video_id': 'video_count',
            'view_count': 'avg_views',
            'like_count': 'avg_likes',
            'comment_count': 'avg_comments',
            'like_view_ratio': 'avg_like_view_ratio',
            'comment_view_ratio': 'avg_comment_view_ratio',
            'views_per_hour': 'avg_views_per_hour'
        })
        
        # Get top channels by video count
        top_by_video_count = channel_stats.sort_values('video_count', ascending=False).head(limit)
        
        # Get top channels by average views (minimum 2 videos)
        top_by_avg_views = channel_stats[channel_stats['video_count'] >= 2]
        top_by_avg_views = top_by_avg_views.sort_values('avg_views', ascending=False).head(limit)
        
        # Convert to JSON-serializable format
        return {
            'top_by_video_count': json.loads(top_by_video_count.to_json(orient='records')),
            'top_by_avg_views': json.loads(top_by_avg_views.to_json(orient='records'))
        }
    
    def analyze_content_features(self, data: Dict[int, pd.DataFrame]) -> Dict:
        """
        Analyze content features like duration, publishing time, etc.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            
        Returns:
            Dict: Content features analysis
        """
        logger.info("Analyzing content features...")
        
        # Combine all DataFrames
        combined_df = pd.concat(data.values(), ignore_index=True)
        
        # Duration analysis
        duration_stats = combined_df.groupby('length_category').agg({
            'video_id': 'count',
            'view_count': 'mean',
            'like_view_ratio': 'mean',
            'views_per_hour': 'mean'
        }).reset_index()
        
        # Rename columns
        duration_stats = duration_stats.rename(columns={
            'video_id': 'video_count',
            'view_count': 'avg_views',
            'like_view_ratio': 'avg_like_view_ratio',
            'views_per_hour': 'avg_views_per_hour'
        })
        
        # Publication day of week analysis
        combined_df['day_of_week'] = pd.to_datetime(combined_df['publish_time']).dt.day_name()
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_stats = combined_df.groupby('day_of_week').agg({
            'video_id': 'count',
            'view_count': 'mean',
            'views_per_hour': 'mean'
        }).reindex(day_order).reset_index()
        
        # Rename columns
        day_stats = day_stats.rename(columns={
            'video_id': 'video_count',
            'view_count': 'avg_views',
            'views_per_hour': 'avg_views_per_hour'
        })
        
        # Publication hour analysis
        combined_df['hour_of_day'] = pd.to_datetime(combined_df['publish_time']).dt.hour
        hour_stats = combined_df.groupby('hour_of_day').agg({
            'video_id': 'count',
            'view_count': 'mean',
            'views_per_hour': 'mean'
        }).reset_index()
        
        # Rename columns
        hour_stats = hour_stats.rename(columns={
            'video_id': 'video_count',
            'view_count': 'avg_views',
            'views_per_hour': 'avg_views_per_hour'
        })
        
        # Title analysis
        title_length_bins = [0, 20, 40, 60, 80, 100, float('inf')]
        title_length_labels = ['0-20', '21-40', '41-60', '61-80', '81-100', '100+']
        combined_df['title_length_category'] = pd.cut(
            combined_df['title_length'], 
            bins=title_length_bins, 
            labels=title_length_labels
        )
        
        title_length_stats = combined_df.groupby('title_length_category').agg({
            'video_id': 'count',
            'view_count': 'mean',
            'views_per_hour': 'mean'
        }).reset_index()
        
        # Rename columns
        title_length_stats = title_length_stats.rename(columns={
            'video_id': 'video_count',
            'view_count': 'avg_views',
            'views_per_hour': 'avg_views_per_hour'
        })
        
        # Convert to JSON-serializable format
        return {
            'duration_stats': json.loads(duration_stats.to_json(orient='records')),
            'day_of_week_stats': json.loads(day_stats.to_json(orient='records')),
            'hour_of_day_stats': json.loads(hour_stats.to_json(orient='records')),
            'title_length_stats': json.loads(title_length_stats.to_json(orient='records'))
        }
    
    def analyze_hashtags_and_tags(self, data: Dict[int, pd.DataFrame], limit: int = 20) -> Dict:
        """
        Analyze hashtags and tags in trending videos.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            limit (int): Maximum number of hashtags/tags to return
            
        Returns:
            Dict: Hashtags and tags analysis
        """
        logger.info(f"Analyzing top {limit} hashtags and tags...")
        
        # Combine all DataFrames
        combined_df = pd.concat(data.values(), ignore_index=True)
        
        # Analyze hashtags
        all_hashtags = []
        for hashtags_list in combined_df['all_hashtags']:
            if isinstance(hashtags_list, list):
                all_hashtags.extend(hashtags_list)
        
        hashtag_counts = Counter(all_hashtags).most_common(limit)
        hashtag_results = [{'hashtag': tag, 'count': count} for tag, count in hashtag_counts]
        
        # Analyze tags
        all_tags = []
        for tags_list in combined_df['tags_list']:
            if isinstance(tags_list, list):
                all_tags.extend(tags_list)
        
        tag_counts = Counter(all_tags).most_common(limit)
        tag_results = [{'tag': tag, 'count': count} for tag, count in tag_counts]
        
        # Hashtags by category
        hashtags_by_category = {}
        for cat_id, df in data.items():
            if cat_id == 0:  # Skip the "All" category to avoid duplication
                continue
            
            cat_hashtags = []
            for hashtags_list in df['all_hashtags']:
                if isinstance(hashtags_list, list):
                    cat_hashtags.extend(hashtags_list)
            
            cat_hashtag_counts = Counter(cat_hashtags).most_common(10)
            hashtags_by_category[str(cat_id)] = [
                {'hashtag': tag, 'count': count} for tag, count in cat_hashtag_counts
            ]
        
        return {
            'top_hashtags': hashtag_results,
            'top_tags': tag_results,
            'hashtags_by_category': hashtags_by_category
        }
    
    def create_visualizations(self, data: Dict[int, pd.DataFrame], analysis_results: Dict, output_dir: str = 'visualizations'):
        """
        Create visualizations for analysis results.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            analysis_results (Dict): Analysis results
            output_dir (str): Directory to save visualizations
        """
        logger.info("Creating visualizations...")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Combine all DataFrames
        combined_df = pd.concat(data.values(), ignore_index=True)
        
        # Set plotting style
        sns.set(style="whitegrid")
        
        # 1. Category Distribution
        plt.figure(figsize=(12, 6))
        category_counts = combined_df['category_name'].value_counts()
        sns.barplot(x=category_counts.values, y=category_counts.index)
        plt.title('Distribution of Trending Videos by Category')
        plt.xlabel('Number of Videos')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/category_distribution.png", dpi=300)
        plt.close()
        
        # 2. Video Duration Distribution
        plt.figure(figsize=(10, 6))
        sns.histplot(combined_df['duration_seconds'] / 60, bins=30, kde=True)
        plt.title('Distribution of Video Duration (Minutes)')
        plt.xlabel('Duration (Minutes)')
        plt.ylabel('Number of Videos')
        plt.xlim(0, 30)  # Limit to 30 minutes for better visibility
        plt.tight_layout()
        plt.savefig(f"{output_dir}/duration_distribution.png", dpi=300)
        plt.close()
        
        # 3. Views vs Likes Scatter Plot
        plt.figure(figsize=(10, 6))
        sns.scatterplot(
            x='view_count', 
            y='like_count',
            hue='category_name',
            data=combined_df.sample(min(1000, len(combined_df)))  # Sample to avoid overcrowding
        )
        plt.title('Views vs Likes for Trending Videos')
        plt.xlabel('View Count')
        plt.ylabel('Like Count')
        plt.ticklabel_format(style='plain', axis='both')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/views_vs_likes.png", dpi=300)
        plt.close()
        
        # 4. Publication Day of Week
        plt.figure(figsize=(10, 6))
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_counts = combined_df['day_of_week'].value_counts().reindex(day_order)
        sns.barplot(x=day_counts.index, y=day_counts.values)
        plt.title('Trending Videos by Publication Day')
        plt.xlabel('Day of Week')
        plt.ylabel('Number of Videos')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/publication_day.png", dpi=300)
        plt.close()
        
        # 5. Top Hashtags
        plt.figure(figsize=(12, 6))
        hashtags_df = pd.DataFrame(analysis_results['hashtags_and_tags']['top_hashtags'])
        if not hashtags_df.empty:
            sns.barplot(x='count', y='hashtag', data=hashtags_df.head(15))
            plt.title('Top 15 Hashtags in Trending Videos')
            plt.xlabel('Count')
            plt.tight_layout()
            plt.savefig(f"{output_dir}/top_hashtags.png", dpi=300)
        plt.close()
        
        # 6. Video Length Category vs Views
        plt.figure(figsize=(10, 6))
        sns.barplot(x='length_category', y='avg_views', data=pd.DataFrame(analysis_results['content_features']['duration_stats']))
        plt.title('Average Views by Video Length')
        plt.xlabel('Video Length')
        plt.ylabel('Average Views')
        plt.ticklabel_format(style='plain', axis='y')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/length_vs_views.png", dpi=300)
        plt.close()
        
        # 7. Correlation Heatmap
        plt.figure(figsize=(12, 10))
        numeric_df = combined_df[['view_count', 'like_count', 'comment_count', 'duration_seconds', 
                                 'like_view_ratio', 'comment_view_ratio', 'views_per_hour']]
        correlation = numeric_df.corr()
        mask = np.triu(correlation)
        sns.heatmap(correlation, annot=True, cmap='coolwarm', linewidths=0.5, mask=mask)
        plt.title('Correlation Between Video Metrics')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/correlation_heatmap.png", dpi=300)
        plt.close()
        
        logger.info(f"Visualizations saved to {output_dir}")
    
    def run_analysis(self, data: Dict[int, pd.DataFrame]) -> Dict:
        """
        Run complete analysis on trending data.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            
        Returns:
            Dict: Complete analysis results
        """
        logger.info("Running complete analysis...")
        
        # Run all analyses
        category_trends = self.analyze_category_trends(data)
        top_videos = self.analyze_top_videos(data)
        top_channels = self.analyze_top_channels(data)
        content_features = self.analyze_content_features(data)
        hashtags_and_tags = self.analyze_hashtags_and_tags(data)
        
        # Combine results
        results = {
            'timestamp': datetime.now().isoformat(),
            'category_trends': category_trends,
            'top_videos': top_videos,
            'top_channels': top_channels,
            'content_features': content_features,
            'hashtags_and_tags': hashtags_and_tags
        }
        
        # Create visualizations
        self.create_visualizations(data, results)
        
        # Upload analysis results to S3
        s3_uri = self.s3_handler.upload_analysis_results(results, datetime.now().strftime('%Y%m%d%H%M%S'))
        results['s3_uri'] = s3_uri
        
        logger.info("Analysis completed successfully")
        return results

def main(config_path: str, data: Optional[Dict[int, pd.DataFrame]] = None) -> Dict:
    """
    Main function to analyze YouTube trending data.
    
    Args:
        config_path (str): Path to the configuration file
        data (Optional[Dict[int, pd.DataFrame]]): Dictionary of DataFrames by category
                                                 If None, data will be loaded from database
    
    Returns:
        Dict: Analysis results
    """
    import yaml
    import os
    
    # Load configuration
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Create analyzer
    analyzer = YouTubeAnalyzer(config)
    
    # If data is not provided, load the latest from the database
    if data is None:
        logger.info("Data not provided, loading from database...")
        db_handler = DatabaseHandler(config)
        
        # Get latest batch_id
        with db_handler.engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT batch_id FROM trending_videos ORDER BY batch_id DESC LIMIT 1"))
            latest_batch = result.fetchone()[0]
        
        # Load data for each category
        data = {}
        with db_handler.engine.connect() as conn:
            # Get all category IDs
            result = conn.execute(text("SELECT DISTINCT category_id FROM trending_videos WHERE batch_id = :batch_id"),
                                {'batch_id': latest_batch})
            category_ids = [row[0] for row in result.fetchall()]
            
            # Load data for each category
            for cat_id in category_ids:
                df = pd.read_sql(
                    text("SELECT * FROM trending_videos WHERE batch_id = :batch_id AND category_id = :category_id"),
                    conn,
                    params={'batch_id': latest_batch, 'category_id': cat_id}
                )
                data[cat_id] = df
    
    # Run analysis
    results = analyzer.run_analysis(data)
    
    return results

if __name__ == "__main__":
    import sys
    import pickle
    import os
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
        
        # If data path is provided, load data from pickle file
        if len(sys.argv) > 2:
            data_path = sys.argv[2]
            with open(data_path, 'rb') as f:
                data = pickle.load(f)
            
            # Run analysis
            results = main(config_path, data)
        else:
            # Load data from database
            results = main(config_path)
        
        # If output path is provided, save the results
        if len(sys.argv) > 3:
            output_path = sys.argv[3]
            with open(output_path, 'wb') as f:
                pickle.dump(results, f)
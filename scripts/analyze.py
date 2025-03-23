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
import os
import tempfile
from sqlalchemy import text

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
        
        # Select only columns that are guaranteed to exist
        columns_to_select = ['video_id', 'title', 'channel_title', 'category_name', 
                            'view_count', 'views_per_hour', 'like_count', 'comment_count']
        # Add optional columns if they exist
        optional_columns = ['duration_seconds', 'publish_time']
        for col in optional_columns:
            if col in top_by_views_per_hour.columns:
                columns_to_select.append(col)
                
        top_by_views_per_hour = top_by_views_per_hour[columns_to_select]
        
        # Get top videos by like-to-view ratio (minimum 10000 views)
        top_by_likes = combined_df[combined_df['view_count'] >= 10000]
        top_by_likes = top_by_likes.sort_values('like_view_ratio', ascending=False).head(limit)
        top_by_likes = top_by_likes[columns_to_select]
        
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
        
        # Ensure required columns exist
        required_columns = ['channel_id', 'channel_title', 'video_id', 'view_count', 
                            'like_count', 'comment_count']
        for col in required_columns:
            if col not in combined_df.columns:
                logger.warning(f"Missing required column '{col}' for channel analysis")
                # Return empty results to avoid errors
                return {
                    'top_by_video_count': [],
                    'top_by_avg_views': []
                }
        
        # Group by channel
        channel_stats = combined_df.groupby(['channel_id', 'channel_title']).agg({
            'video_id': 'count',
            'view_count': 'mean',
            'like_count': 'mean',
            'comment_count': 'mean'
        }).reset_index()
        
        # Add derived metrics if possible
        if 'like_view_ratio' in combined_df.columns and 'comment_view_ratio' in combined_df.columns:
            channel_stats_extended = combined_df.groupby(['channel_id', 'channel_title']).agg({
                'like_view_ratio': 'mean',
                'comment_view_ratio': 'mean'
            }).reset_index()
            
            # Merge the additional metrics
            channel_stats = pd.merge(channel_stats, channel_stats_extended, 
                                    on=['channel_id', 'channel_title'])
        
        # Add views_per_hour if it exists
        if 'views_per_hour' in combined_df.columns:
            views_per_hour_stats = combined_df.groupby(['channel_id', 'channel_title']).agg({
                'views_per_hour': 'mean'
            }).reset_index()
            
            # Merge the additional metric
            channel_stats = pd.merge(channel_stats, views_per_hour_stats, 
                                    on=['channel_id', 'channel_title'])
        
        # Rename columns for clarity
        channel_stats = channel_stats.rename(columns={
            'video_id': 'video_count',
            'view_count': 'avg_views',
            'like_count': 'avg_likes',
            'comment_count': 'avg_comments'
        })
        
        # Get top channels by video count
        top_by_video_count = channel_stats.sort_values('video_count', ascending=False).head(limit)
        
        # Get top channels by average views (minimum 2 videos)
        if len(channel_stats[channel_stats['video_count'] >= 2]) > 0:
            top_by_avg_views = channel_stats[channel_stats['video_count'] >= 2]
            top_by_avg_views = top_by_avg_views.sort_values('avg_views', ascending=False).head(limit)
        else:
            # If no channels have at least 2 videos, just use all channels
            top_by_avg_views = channel_stats.sort_values('avg_views', ascending=False).head(limit)
        
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
        
        results = {}
        
        # Duration analysis - only if length_category exists
        if 'length_category' in combined_df.columns:
            # Make sure all required metrics exist
            metrics = ['view_count', 'like_view_ratio', 'views_per_hour']
            valid_metrics = [m for m in metrics if m in combined_df.columns]
            
            # Add video_id count to always have at least one metric
            valid_metrics.append('video_id')
            
            duration_stats = combined_df.groupby('length_category').agg({
                'video_id': 'count',
                **{metric: 'mean' for metric in valid_metrics if metric != 'video_id'}
            }).reset_index()
            
            # Rename columns
            duration_stats = duration_stats.rename(columns={
                'video_id': 'video_count',
                'view_count': 'avg_views',
                'like_view_ratio': 'avg_like_view_ratio',
                'views_per_hour': 'avg_views_per_hour'
            })
            
            results['duration_stats'] = json.loads(duration_stats.to_json(orient='records'))
        else:
            logger.warning("Missing 'length_category' column for duration analysis")
            results['duration_stats'] = []
        
        # Publication day of week analysis - create if missing
        if 'publish_time' in combined_df.columns:
            try:
                combined_df['day_of_week'] = pd.to_datetime(combined_df['publish_time']).dt.day_name()
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                
                metrics = ['view_count', 'views_per_hour']
                valid_metrics = [m for m in metrics if m in combined_df.columns]
                valid_metrics.append('video_id')
                
                day_stats = combined_df.groupby('day_of_week').agg({
                    'video_id': 'count',
                    **{metric: 'mean' for metric in valid_metrics if metric != 'video_id'}
                }).reindex(day_order).reset_index()
                
                # Rename columns
                day_stats = day_stats.rename(columns={
                    'video_id': 'video_count',
                    'view_count': 'avg_views',
                    'views_per_hour': 'avg_views_per_hour'
                })
                
                results['day_of_week_stats'] = json.loads(day_stats.to_json(orient='records'))
            except Exception as e:
                logger.warning(f"Error in day of week analysis: {str(e)}")
                results['day_of_week_stats'] = []
        else:
            logger.warning("Missing 'publish_time' column for day of week analysis")
            results['day_of_week_stats'] = []
        
        # Publication hour analysis - create if missing
        if 'publish_time' in combined_df.columns:
            try:
                combined_df['hour_of_day'] = pd.to_datetime(combined_df['publish_time']).dt.hour
                
                metrics = ['view_count', 'views_per_hour']
                valid_metrics = [m for m in metrics if m in combined_df.columns]
                valid_metrics.append('video_id')
                
                hour_stats = combined_df.groupby('hour_of_day').agg({
                    'video_id': 'count',
                    **{metric: 'mean' for metric in valid_metrics if metric != 'video_id'}
                }).reset_index()
                
                # Rename columns
                hour_stats = hour_stats.rename(columns={
                    'video_id': 'video_count',
                    'view_count': 'avg_views',
                    'views_per_hour': 'avg_views_per_hour'
                })
                
                results['hour_of_day_stats'] = json.loads(hour_stats.to_json(orient='records'))
            except Exception as e:
                logger.warning(f"Error in hour of day analysis: {str(e)}")
                results['hour_of_day_stats'] = []
        else:
            logger.warning("Missing 'publish_time' column for hour of day analysis")
            results['hour_of_day_stats'] = []
        
        # Title analysis - only if title_length exists
        if 'title_length' in combined_df.columns:
            try:
                title_length_bins = [0, 20, 40, 60, 80, 100, float('inf')]
                title_length_labels = ['0-20', '21-40', '41-60', '61-80', '81-100', '100+']
                combined_df['title_length_category'] = pd.cut(
                    combined_df['title_length'], 
                    bins=title_length_bins, 
                    labels=title_length_labels
                )
                
                metrics = ['view_count', 'views_per_hour']
                valid_metrics = [m for m in metrics if m in combined_df.columns]
                valid_metrics.append('video_id')
                
                title_length_stats = combined_df.groupby('title_length_category').agg({
                    'video_id': 'count',
                    **{metric: 'mean' for metric in valid_metrics if metric != 'video_id'}
                }).reset_index()
                
                # Rename columns
                title_length_stats = title_length_stats.rename(columns={
                    'video_id': 'video_count',
                    'view_count': 'avg_views',
                    'views_per_hour': 'avg_views_per_hour'
                })
                
                results['title_length_stats'] = json.loads(title_length_stats.to_json(orient='records'))
            except Exception as e:
                logger.warning(f"Error in title length analysis: {str(e)}")
                results['title_length_stats'] = []
        else:
            logger.warning("Missing 'title_length' column for title length analysis")
            results['title_length_stats'] = []
        
        return results
    
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
        
        # Initialize results dictionary
        results = {
            'top_hashtags': [],
            'top_tags': [],
            'hashtags_by_category': {}
        }
        
        # Analyze hashtags
        if 'all_hashtags' in combined_df.columns:
            try:
                all_hashtags = []
                for hashtags_list in combined_df['all_hashtags']:
                    if isinstance(hashtags_list, list):
                        all_hashtags.extend(hashtags_list)
                    elif isinstance(hashtags_list, str):
                        # Try to parse JSON string
                        try:
                            parsed = json.loads(hashtags_list)
                            if isinstance(parsed, list):
                                all_hashtags.extend(parsed)
                        except:
                            # Not valid JSON, might be a single hashtag
                            all_hashtags.append(hashtags_list)
                
                hashtag_counts = Counter(all_hashtags).most_common(limit)
                results['top_hashtags'] = [{'hashtag': tag, 'count': count} for tag, count in hashtag_counts]
            except Exception as e:
                logger.warning(f"Error analyzing hashtags: {str(e)}")
        else:
            logger.warning("Missing 'all_hashtags' column for hashtag analysis")
        
        # Analyze tags
        tags_column = None
        if 'tags_list' in combined_df.columns:
            tags_column = 'tags_list'
        elif 'tags' in combined_df.columns:
            tags_column = 'tags'
            
        if tags_column:
            try:
                all_tags = []
                for tags_list in combined_df[tags_column]:
                    if isinstance(tags_list, list):
                        all_tags.extend(tags_list)
                    elif isinstance(tags_list, str):
                        # Try to parse JSON string
                        try:
                            parsed = json.loads(tags_list)
                            if isinstance(parsed, list):
                                all_tags.extend(parsed)
                        except:
                            # Not valid JSON, might be a single tag
                            all_tags.append(tags_list)
                
                tag_counts = Counter(all_tags).most_common(limit)
                results['top_tags'] = [{'tag': tag, 'count': count} for tag, count in tag_counts]
            except Exception as e:
                logger.warning(f"Error analyzing tags: {str(e)}")
        else:
            logger.warning("Missing tags column for tag analysis")
        
        # Hashtags by category - skip if all_hashtags missing
        if 'all_hashtags' in combined_df.columns and 'category_id' in combined_df.columns:
            try:
                for cat_id, df in data.items():
                    if cat_id == 0:  # Skip the "All" category
                        continue
                    
                    if 'all_hashtags' not in df.columns:
                        continue
                        
                    cat_hashtags = []
                    for hashtags_list in df['all_hashtags']:
                        if isinstance(hashtags_list, list):
                            cat_hashtags.extend(hashtags_list)
                        elif isinstance(hashtags_list, str):
                            # Try to parse JSON string
                            try:
                                parsed = json.loads(hashtags_list)
                                if isinstance(parsed, list):
                                    cat_hashtags.extend(parsed)
                            except:
                                # Not valid JSON, might be a single hashtag
                                cat_hashtags.append(hashtags_list)
                    
                    cat_hashtag_counts = Counter(cat_hashtags).most_common(10)
                    results['hashtags_by_category'][str(cat_id)] = [
                        {'hashtag': tag, 'count': count} for tag, count in cat_hashtag_counts
                    ]
            except Exception as e:
                logger.warning(f"Error analyzing hashtags by category: {str(e)}")
        
        return results
    
    def create_visualizations(self, data: Dict[int, pd.DataFrame], analysis_results: Dict, output_dir: str = 'visualizations'):
        """
        Create visualizations for analysis results.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            analysis_results (Dict): Analysis results
            output_dir (str): Directory to save visualizations
        """
        logger.info("Creating visualizations...")
        
        # Try to create output directory with fallback options
        try:
            # Try using specified directory
            os.makedirs(output_dir, exist_ok=True)
        except PermissionError:
            # If permission error, try using a directory in user's home
            try:
                home_dir = os.path.expanduser("~")
                output_dir = os.path.join(home_dir, "youtube_trending_visualizations")
                logger.info(f"Permission error with original directory, using: {output_dir}")
                os.makedirs(output_dir, exist_ok=True)
            except PermissionError:
                # If still permission error, use temporary directory
                output_dir = tempfile.mkdtemp()
                logger.info(f"Using temporary directory for visualizations: {output_dir}")
        
        # Combine all DataFrames
        combined_df = pd.concat(data.values(), ignore_index=True)
        
        # Set plotting style
        sns.set(style="whitegrid")
        
        # 1. Category Distribution
        plt.figure(figsize=(12, 6))
        if 'category_name' in combined_df.columns:
            category_counts = combined_df['category_name'].value_counts()
            sns.barplot(x=category_counts.values, y=category_counts.index)
            plt.title('Distribution of Trending Videos by Category')
            plt.xlabel('Number of Videos')
        else:
            plt.title('Category Distribution Not Available')
            plt.text(0.5, 0.5, 'Category data unavailable', 
                    horizontalalignment='center', verticalalignment='center',
                    transform=plt.gca().transAxes)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/category_distribution.png", dpi=300)
        plt.close()
        
        # 2. Video Duration Distribution
        plt.figure(figsize=(10, 6))
        if 'duration_seconds' in combined_df.columns:
            # Convert to minutes for better visualization
            duration_minutes = combined_df['duration_seconds'] / 60
            
            # Remove extreme outliers for better visualization
            duration_minutes = duration_minutes[duration_minutes < duration_minutes.quantile(0.99)]
            
            sns.histplot(duration_minutes, bins=30, kde=True)
            plt.title('Distribution of Video Duration (Minutes)')
            plt.xlabel('Duration (Minutes)')
            plt.ylabel('Number of Videos')
            plt.xlim(0, duration_minutes.max() * 1.1)  # Better x-axis limit
        else:
            plt.title('Duration Distribution Not Available')
            plt.text(0.5, 0.5, 'Duration data unavailable', 
                    horizontalalignment='center', verticalalignment='center',
                    transform=plt.gca().transAxes)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/duration_distribution.png", dpi=300)
        plt.close()
        
        # 3. Views vs Likes Scatter Plot
        plt.figure(figsize=(10, 6))
        if 'view_count' in combined_df.columns and 'like_count' in combined_df.columns:
            # Sample to avoid overcrowding if dataset is large
            sample_size = min(1000, len(combined_df))
            sample_df = combined_df.sample(sample_size)
            
            if 'category_name' in sample_df.columns:
                sns.scatterplot(
                    x='view_count', 
                    y='like_count',
                    hue='category_name',
                    data=sample_df
                )
            else:
                sns.scatterplot(
                    x='view_count', 
                    y='like_count',
                    data=sample_df
                )
                
            plt.title('Views vs Likes for Trending Videos')
            plt.xlabel('View Count')
            plt.ylabel('Like Count')
            plt.ticklabel_format(style='plain', axis='both')
        else:
            plt.title('Views vs Likes Not Available')
            plt.text(0.5, 0.5, 'View and like data unavailable', 
                    horizontalalignment='center', verticalalignment='center',
                    transform=plt.gca().transAxes)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/views_vs_likes.png", dpi=300)
        plt.close()
        
        # 4. Publication Day of Week
        plt.figure(figsize=(10, 6))
        if 'day_of_week' not in combined_df.columns:
            # Create the day_of_week column if missing
            try:
                combined_df['day_of_week'] = pd.to_datetime(combined_df['publish_time']).dt.day_name()
                logger.info("Created missing 'day_of_week' column")
            except Exception as e:
                logger.warning(f"Could not create 'day_of_week' column: {str(e)}")
                # Create an alternative simple plot
                plt.title('Publication Day Data Not Available')
                plt.text(0.5, 0.5, 'Publication day data unavailable', 
                        horizontalalignment='center', verticalalignment='center',
                        transform=plt.gca().transAxes)
                plt.tight_layout()
                plt.savefig(f"{output_dir}/publication_day.png", dpi=300)
                plt.close()
                
                # Skip to the next visualization
                logger.info("Skipping publication day visualization")
                
        if 'day_of_week' in combined_df.columns:
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
        if 'hashtags_and_tags' in analysis_results and analysis_results['hashtags_and_tags']['top_hashtags']:
            hashtags_df = pd.DataFrame(analysis_results['hashtags_and_tags']['top_hashtags'])
            if not hashtags_df.empty:
                sns.barplot(x='count', y='hashtag', data=hashtags_df.head(15))
                plt.title('Top 15 Hashtags in Trending Videos')
                plt.xlabel('Count')
            else:
                plt.title('No Hashtags Found')
                plt.text(0.5, 0.5, 'No hashtags in the dataset', 
                        horizontalalignment='center', verticalalignment='center',
                        transform=plt.gca().transAxes)
        else:
            plt.title('Hashtag Data Not Available')
            plt.text(0.5, 0.5, 'Hashtag data unavailable', 
                    horizontalalignment='center', verticalalignment='center',
                    transform=plt.gca().transAxes)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/top_hashtags.png", dpi=300)
        plt.close()
        
        # 6. Video Length Category vs Views
        plt.figure(figsize=(10, 6))
        if 'content_features' in analysis_results and analysis_results['content_features']['duration_stats']:
            duration_stats_df = pd.DataFrame(analysis_results['content_features']['duration_stats'])
            if 'avg_views' in duration_stats_df.columns:
                sns.barplot(x='length_category', y='avg_views', data=duration_stats_df)
                plt.title('Average Views by Video Length')
                plt.xlabel('Video Length')
                plt.ylabel('Average Views')
                plt.ticklabel_format(style='plain', axis='y')
            else:
                plt.title('Average Views by Length Not Available')
                plt.text(0.5, 0.5, 'View data by length unavailable', 
                        horizontalalignment='center', verticalalignment='center',
                        transform=plt.gca().transAxes)
        else:
            plt.title('Length vs Views Data Not Available')
            plt.text(0.5, 0.5, 'Length category data unavailable', 
                    horizontalalignment='center', verticalalignment='center',
                    transform=plt.gca().transAxes)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/length_vs_views.png", dpi=300)
        plt.close()
        
        # 7. Correlation Heatmap
        plt.figure(figsize=(12, 10))
        metric_columns = ['view_count', 'like_count', 'comment_count', 'duration_seconds', 
                        'like_view_ratio', 'comment_view_ratio', 'views_per_hour']
        available_metrics = [col for col in metric_columns if col in combined_df.columns]
        
        if len(available_metrics) >= 2:
            numeric_df = combined_df[available_metrics]
            numeric_df = numeric_df.apply(pd.to_numeric, errors='coerce')
            
            # Drop any columns that have all NaN values
            numeric_df = numeric_df.dropna(axis=1, how='all')
            
            # Compute correlation and filter out extreme values for better visualization
            correlation = numeric_df.corr()
            
            # Create mask for the upper triangle
            mask = np.triu(correlation)
            
            try:
                sns.heatmap(correlation, annot=True, cmap='coolwarm', linewidths=0.5, mask=mask)
                plt.title('Correlation Between Video Metrics')
            except Exception as e:
                logger.warning(f"Error creating correlation heatmap: {str(e)}")
                plt.title('Correlation Heatmap Error')
                plt.text(0.5, 0.5, f'Error creating correlation heatmap: {str(e)}', 
                        horizontalalignment='center', verticalalignment='center',
                        transform=plt.gca().transAxes)
        else:
            plt.title('Correlation Data Not Available')
            plt.text(0.5, 0.5, 'Not enough numeric metrics available', 
                    horizontalalignment='center', verticalalignment='center',
                    transform=plt.gca().transAxes)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/correlation_heatmap.png", dpi=300)
        plt.close()
        
        logger.info(f"Visualizations saved to {output_dir}")
    
    def run_analysis(self, data: Dict[int, pd.DataFrame], output_dir: str = None) -> Dict:
        """
        Run complete analysis on trending data.
        
        Args:
            data (Dict[int, pd.DataFrame]): Dictionary of DataFrames by category
            output_dir (str, optional): Directory to save visualizations. Default is None.
            
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
        try:
            if output_dir:
                self.create_visualizations(data, results, output_dir)
            else:
                self.create_visualizations(data, results)
        except Exception as e:
            logger.warning(f"Error creating visualizations: {str(e)}")
        
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
            latest_batch = result.fetchone()
            if latest_batch:
                latest_batch = latest_batch[0]
            else:
                logger.error("No data found in database")
                raise ValueError("No data found in database")
        
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
            try:
                with open(data_path, 'rb') as f:
                    data = pickle.load(f)
                
                # Run analysis
                results = main(config_path, data)
            except Exception as e:
                logger.error(f"Error running analysis: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                sys.exit(1)
        else:
            # Load data from database
            try:
                results = main(config_path)
            except Exception as e:
                logger.error(f"Error running analysis: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                sys.exit(1)
        
        # If output path is provided, save the results
        if len(sys.argv) > 3:
            output_path = sys.argv[3]
            with open(output_path, 'wb') as f:
                pickle.dump(results, f)
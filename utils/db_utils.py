#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilities for interacting with the database.
"""

import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, JSON, text
import logging
from typing import Dict, List, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseHandler:
    """
    Class to handle database operations for the YouTube trending analysis.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize database handler with configuration.
        
        Args:
            config (Dict): Configuration dictionary from config.yaml
        """
        self.db_type = config['database']['type']
        self.host = config['database']['host']
        self.port = config['database']['port']
        self.database = config['database']['database']
        self.username = config['database']['username']
        self.password = os.environ.get('DB_PASSWORD')
        
        if not self.password and self.db_type == 'postgres':
            logger.warning("DB_PASSWORD environment variable not set. "
                          "Will try to connect without password.")
        
        self.engine = self._create_engine()
        self.metadata = MetaData()
        
        # Define tables
        self._define_tables()
    
    def _create_engine(self):
        """
        Create database engine based on configuration.
        
        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy engine
        """
        try:
            if self.db_type == 'sqlite':
                return create_engine(f'sqlite:///{self.database}.db')
            elif self.db_type == 'postgres':
                if self.password:
                    return create_engine(
                        f'postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
                    )
                else:
                    return create_engine(
                        f'postgresql://{self.username}@{self.host}:{self.port}/{self.database}'
                    )
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
        except Exception as e:
            logger.error(f"Error creating database engine: {str(e)}")
            raise
    
    def _define_tables(self):
        """
        Define database tables.
        """
        # Trending videos table
        self.trending_videos = Table(
            'trending_videos', 
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('batch_id', String, index=True),
            Column('video_id', String, index=True),
            Column('title', String),
            Column('channel_id', String, index=True),
            Column('channel_title', String),
            Column('category_id', Integer, index=True),
            Column('category_name', String),
            Column('publish_time', DateTime, index=True),
            Column('extracted_at', DateTime, index=True),
            Column('view_count', Integer),
            Column('like_count', Integer),
            Column('comment_count', Integer),
            Column('duration_seconds', Integer),
            Column('length_category', String),
            Column('hours_since_published', Float),
            Column('views_per_hour', Float),
            Column('like_view_ratio', Float),
            Column('comment_view_ratio', Float),
            Column('thumbnail_url', String),
            Column('tags', JSON),
            Column('title_hashtags', JSON),
            Column('description_hashtags', JSON),
            Column('all_hashtags', JSON)
        )
        
        # Channel statistics table
        self.channel_stats = Table(
            'channel_stats',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('batch_id', String, index=True),
            Column('channel_id', String, index=True),
            Column('channel_title', String),
            Column('video_count', Integer),
            Column('avg_views', Float),
            Column('avg_likes', Float),
            Column('avg_comments', Float),
            Column('avg_like_view_ratio', Float),
            Column('avg_comment_view_ratio', Float),
            Column('extracted_at', DateTime, index=True)
        )
        
        # Trends summary table
        self.trends_summary = Table(
            'trends_summary',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('batch_id', String, index=True),
            Column('category_id', Integer, index=True),
            Column('category_name', String),
            Column('video_count', Integer),
            Column('avg_views', Float),
            Column('avg_likes', Float),
            Column('avg_comments', Float),
            Column('avg_duration', Float),
            Column('avg_like_view_ratio', Float),
            Column('avg_comment_view_ratio', Float),
            Column('avg_views_per_hour', Float),
            Column('extracted_at', DateTime, index=True)
        )
        
        # Hashtags table
        self.hashtags = Table(
            'hashtags',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('batch_id', String, index=True),
            Column('hashtag', String, index=True),
            Column('count', Integer),
            Column('category_id', Integer, index=True),
            Column('category_name', String),
            Column('extracted_at', DateTime, index=True)
        )
    
    def create_tables(self):
        """
        Create all defined tables in the database if they don't exist.
        """
        try:
            self.metadata.create_all(self.engine)
            logger.info("Database tables created successfully.")
        except Exception as e:
            logger.error(f"Error creating database tables: {str(e)}")
            raise
    
    def store_trending_videos(self, df: pd.DataFrame):
        """
        Store trending videos in the database.
        
        Args:
            df (pd.DataFrame): DataFrame with trending videos data
        """
        try:
            # Prepare data for insertion
            data = df.copy()
            
            # Convert JSON columns to proper format
            json_columns = ['tags_list', 'title_hashtags', 'description_hashtags', 'all_hashtags']
            for col in json_columns:
                if col in data.columns:
                    # Rename 'tags_list' to 'tags' for the database
                    if col == 'tags_list':
                        data = data.rename(columns={'tags_list': 'tags'})
                        col = 'tags'
                    
                    # Convert lists to JSON strings for SQLite
                    if self.db_type == 'sqlite':
                        import json
                        data[col] = data[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
            
            # Insert data
            with self.engine.connect() as conn:
                # Use pandas to_sql for bulk insert
                data.to_sql('trending_videos', conn, if_exists='append', index=False, 
                            method='multi', chunksize=1000)
            
            logger.info(f"Successfully stored {len(df)} trending videos in the database.")
        except Exception as e:
            logger.error(f"Error storing trending videos in database: {str(e)}")
            raise
    
    def calculate_channel_stats(self, batch_id: str):
        """
        Calculate channel statistics and store them in the database.
        
        Args:
            batch_id (str): Batch ID to process
        """
        try:
            query = f"""
            INSERT INTO channel_stats (
                batch_id, channel_id, channel_title, video_count, avg_views, avg_likes, 
                avg_comments, avg_like_view_ratio, avg_comment_view_ratio, extracted_at
            )
            SELECT 
                '{batch_id}' as batch_id,
                channel_id,
                channel_title,
                COUNT(*) as video_count,
                AVG(view_count) as avg_views,
                AVG(like_count) as avg_likes,
                AVG(comment_count) as avg_comments,
                AVG(like_view_ratio) as avg_like_view_ratio,
                AVG(comment_view_ratio) as avg_comment_view_ratio,
                MAX(extracted_at) as extracted_at
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
            GROUP BY channel_id, channel_title
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            
            logger.info(f"Successfully calculated channel statistics for batch {batch_id}.")
        except Exception as e:
            logger.error(f"Error calculating channel statistics: {str(e)}")
            raise
    
    def calculate_trends_summary(self, batch_id: str):
        """
        Calculate trends summary and store it in the database.
        
        Args:
            batch_id (str): Batch ID to process
        """
        try:
            query = f"""
            INSERT INTO trends_summary (
                batch_id, category_id, category_name, video_count, avg_views, avg_likes, 
                avg_comments, avg_duration, avg_like_view_ratio, avg_comment_view_ratio, 
                avg_views_per_hour, extracted_at
            )
            SELECT 
                '{batch_id}' as batch_id,
                category_id,
                category_name,
                COUNT(*) as video_count,
                AVG(view_count) as avg_views,
                AVG(like_count) as avg_likes,
                AVG(comment_count) as avg_comments,
                AVG(duration_seconds) as avg_duration,
                AVG(like_view_ratio) as avg_like_view_ratio,
                AVG(comment_view_ratio) as avg_comment_view_ratio,
                AVG(views_per_hour) as avg_views_per_hour,
                MAX(extracted_at) as extracted_at
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
            GROUP BY category_id, category_name
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            
            logger.info(f"Successfully calculated trends summary for batch {batch_id}.")
        except Exception as e:
            logger.error(f"Error calculating trends summary: {str(e)}")
            raise
    
    def calculate_hashtag_stats(self, batch_id: str):
        """
        Calculate hashtag statistics and store them in the database.
        
        Args:
            batch_id (str): Batch ID to process
        """
        try:
            # Use a database-specific approach for working with arrays/JSON
            if self.db_type == 'postgres':
                # For PostgreSQL, we can use unnest to explode the array
                query = f"""
                INSERT INTO hashtags (
                    batch_id, hashtag, count, category_id, category_name, extracted_at
                )
                SELECT 
                    '{batch_id}' as batch_id,
                    hashtag,
                    COUNT(*) as count,
                    category_id,
                    category_name,
                    MAX(extracted_at) as extracted_at
                FROM (
                    SELECT 
                        category_id,
                        category_name,
                        unnest(all_hashtags) as hashtag,
                        extracted_at
                    FROM trending_videos
                    WHERE batch_id = '{batch_id}'
                ) as hashtags_exploded
                GROUP BY hashtag, category_id, category_name
                ORDER BY count DESC
                """
            else:
                # For SQLite, we need to do this in Python
                logger.info("SQLite detected, calculating hashtag stats in Python")
                with self.engine.connect() as conn:
                    df = pd.read_sql(
                        f"SELECT category_id, category_name, all_hashtags, extracted_at FROM trending_videos WHERE batch_id = '{batch_id}'",
                        conn
                    )
                
                # Process hashtags
                import json
                hashtags_data = []
                
                for _, row in df.iterrows():
                    hashtags = json.loads(row['all_hashtags']) if isinstance(row['all_hashtags'], str) else row['all_hashtags']
                    for hashtag in hashtags:
                        hashtags_data.append({
                            'batch_id': batch_id,
                            'hashtag': hashtag,
                            'category_id': row['category_id'],
                            'category_name': row['category_name'],
                            'extracted_at': row['extracted_at']
                        })
                
                # Create DataFrame and count occurrences
                if hashtags_data:
                    hashtags_df = pd.DataFrame(hashtags_data)
                    hashtag_counts = hashtags_df.groupby(['hashtag', 'category_id', 'category_name']).size().reset_index(name='count')
                    hashtag_counts['batch_id'] = batch_id
                    hashtag_counts['extracted_at'] = pd.Timestamp.now()
                    
                    # Insert into database
                    with self.engine.connect() as conn:
                        hashtag_counts.to_sql('hashtags', conn, if_exists='append', index=False)
                
                logger.info(f"Successfully calculated hashtag statistics for batch {batch_id}.")
                return
            
            # Execute the query for PostgreSQL
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            
            logger.info(f"Successfully calculated hashtag statistics for batch {batch_id}.")
        except Exception as e:
            logger.error(f"Error calculating hashtag statistics: {str(e)}")
            raise
    
    def get_trending_videos(self, limit: int = 100, category_id: Optional[int] = None, 
                           batch_id: Optional[str] = None) -> pd.DataFrame:
        """
        Get trending videos from the database.
        
        Args:
            limit (int): Maximum number of records to return
            category_id (Optional[int]): Filter by category ID
            batch_id (Optional[str]): Filter by batch ID
            
        Returns:
            pd.DataFrame: DataFrame with trending videos data
        """
        try:
            query = "SELECT * FROM trending_videos"
            conditions = []
            
            if category_id is not None:
                conditions.append(f"category_id = {category_id}")
            
            if batch_id is not None:
                conditions.append(f"batch_id = '{batch_id}'")
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += f" ORDER BY views_per_hour DESC LIMIT {limit}"
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            
            return df
        except Exception as e:
            logger.error(f"Error getting trending videos from database: {str(e)}")
            raise

def main(config_path: str):
    """
    Main function to test database utilities.
    
    Args:
        config_path (str): Path to the configuration file
    """
    import yaml
    
    # Load configuration
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Create database handler
    db_handler = DatabaseHandler(config)
    
    # Create tables
    db_handler.create_tables()
    
    print("Database tables created successfully.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "../config/config.yaml"
    
    main(config_path)
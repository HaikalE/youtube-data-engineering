#!/usr/bin/env python3
"""
Script to debug database loading issues by directly inserting test data
"""

import os
import sys
import yaml
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project directory to the path
sys.path.append(os.getcwd())

# Import the database handler
try:
    from utils.db_utils import DatabaseHandler
    logger.info("Successfully imported DatabaseHandler")
except Exception as e:
    logger.error(f"Failed to import DatabaseHandler: {e}")
    sys.exit(1)

def create_test_data():
    """Create a simple test DataFrame to insert into the database"""
    logger.info("Creating test data")
    
    # Create a timestamp for batch_id
    batch_id = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Create sample data (just 3 rows for testing)
    data = [
        {
            'batch_id': batch_id,
            'video_id': 'test_vid_1',
            'title': 'Test Video 1',
            'channel_id': 'test_channel_1',
            'channel_title': 'Test Channel',
            'category_id': 1,
            'category_name': 'Music',
            'publish_time': datetime.now().isoformat(),
            'extracted_at': datetime.now().isoformat(),
            'view_count': 1000,
            'like_count': 100,
            'comment_count': 10,
            'duration_seconds': 300,
            'length_category': '1-5 min',
        },
        {
            'batch_id': batch_id,
            'video_id': 'test_vid_2',
            'title': 'Test Video 2',
            'channel_id': 'test_channel_1',
            'channel_title': 'Test Channel',
            'category_id': 2,
            'category_name': 'Gaming',
            'publish_time': datetime.now().isoformat(),
            'extracted_at': datetime.now().isoformat(),
            'view_count': 2000,
            'like_count': 200,
            'comment_count': 20,
            'duration_seconds': 600,
            'length_category': '5-10 min',
        },
        {
            'batch_id': batch_id,
            'video_id': 'test_vid_3',
            'title': 'Test Video 3',
            'channel_id': 'test_channel_2',
            'channel_title': 'Another Channel',
            'category_id': 3,
            'category_name': 'Entertainment',
            'publish_time': datetime.now().isoformat(),
            'extracted_at': datetime.now().isoformat(),
            'view_count': 3000,
            'like_count': 300,
            'comment_count': 30,
            'duration_seconds': 900,
            'length_category': '10-20 min',
        }
    ]
    
    return pd.DataFrame(data), batch_id

def main():
    """Main function to test database insertion"""
    try:
        # Load config
        config_path = os.path.join(os.getcwd(), 'config', 'config.yaml')
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Loaded config from {config_path}")
        
        # Create database handler
        db_handler = DatabaseHandler(config)
        logger.info("Created DatabaseHandler")
        
        # Create tables if they don't exist
        db_handler.create_tables()
        logger.info("Created database tables if they didn't exist")
        
        # Create test data
        test_df, batch_id = create_test_data()
        logger.info(f"Created test data with batch_id: {batch_id}")
        
        # Log the data we're trying to insert
        logger.info(f"Test data columns: {test_df.columns.tolist()}")
        logger.info(f"Test data types: {test_df.dtypes}")
        
        # Try to store the data
        logger.info("Attempting to store data in database...")
        db_handler.store_trending_videos(test_df)
        logger.info("✅ Successfully stored test data in database")
        
        # Try to calculate statistics
        try:
            logger.info("Calculating channel statistics...")
            db_handler.calculate_channel_stats(batch_id)
            
            logger.info("Calculating trends summary...")
            db_handler.calculate_trends_summary(batch_id)
            
            logger.info("Calculating hashtag statistics...")
            db_handler.calculate_hashtag_stats(batch_id)
            
            logger.info("✅ All statistics calculated successfully")
        except Exception as e:
            logger.error(f"❌ Error calculating statistics: {e}")
        
        # Verify the data was inserted
        with db_handler.engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM trending_videos")
            count = result.fetchone()[0]
            logger.info(f"Number of records in trending_videos table: {count}")
            
            result = conn.execute(f"SELECT COUNT(*) FROM trending_videos WHERE batch_id = '{batch_id}'")
            batch_count = result.fetchone()[0]
            logger.info(f"Number of records with batch_id {batch_id}: {batch_count}")
            
            if batch_count == len(test_df):
                logger.info("✅ All test records were inserted successfully")
            else:
                logger.error(f"❌ Expected {len(test_df)} records, but found {batch_count}")
        
        print("\nDebug successful! Data was inserted into the database.")
        print(f"Check your database with 'python3 check_db.py' to verify.")
        
    except Exception as e:
        logger.error(f"❌ Error during debugging: {e}")
        import traceback
        logger.error(traceback.format_exc())
        print("\nDebug failed! See error above.")

if __name__ == "__main__":
    main()
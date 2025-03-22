#!/usr/bin/env python3
"""
Script to directly load the processed pickle data into the database
"""

import os
import sys
import yaml
import pickle
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project directory to the path
sys.path.append(os.getcwd())

# Import handlers
try:
    from utils.db_utils import DatabaseHandler
    logger.info("Successfully imported DatabaseHandler")
except Exception as e:
    logger.error(f"Failed to import DatabaseHandler: {e}")
    sys.exit(1)

def main():
    """Main function to load pickle data to database"""
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
        
        # Load the processed data pickle
        pickle_path = 'data/processed_data.pkl'
        if not os.path.exists(pickle_path):
            logger.error(f"Pickle file not found: {pickle_path}")
            print(f"ERROR: {pickle_path} does not exist! Run the pipeline first.")
            sys.exit(1)
            
        with open(pickle_path, 'rb') as f:
            processed_data = pickle.load(f)
        
        logger.info(f"Loaded processed data from {pickle_path}")
        logger.info(f"Processed data contains {len(processed_data)} categories")
        
        # Combine all DataFrames
        dfs_to_combine = []
        batch_id = datetime.now().strftime('%Y%m%d%H%M%S')
        
        for cat_id, df in processed_data.items():
            # Add batch_id if not present
            if 'batch_id' not in df.columns:
                df['batch_id'] = batch_id
                
            logger.info(f"Category {cat_id}: {len(df)} rows, columns: {df.columns.tolist()}")
            dfs_to_combine.append(df)
            
        if not dfs_to_combine:
            logger.error("No DataFrames to combine!")
            print("ERROR: No data found in the pickle file!")
            sys.exit(1)
            
        combined_df = pd.concat(dfs_to_combine, ignore_index=True)
        logger.info(f"Combined DataFrame has {len(combined_df)} rows")
        
        # Ensure critical columns are not null
        for col in ['video_id', 'title', 'channel_id', 'category_id']:
            if col in combined_df.columns:
                if combined_df[col].isnull().any():
                    logger.warning(f"Found null values in column {col}, filling with defaults")
                    if col == 'video_id':
                        combined_df[col] = combined_df[col].fillna(f"unknown_{batch_id}")
                    elif col == 'title':
                        combined_df[col] = combined_df[col].fillna("Unknown Title")
                    elif col == 'channel_id':
                        combined_df[col] = combined_df[col].fillna(f"unknown_channel_{batch_id}")
                    elif col == 'category_id':
                        combined_df[col] = combined_df[col].fillna(0)
        
        # Convert any list-type columns to JSON strings
        for col in combined_df.columns:
            if combined_df[col].dtype == 'object':
                combined_df[col] = combined_df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                )
        
        # Try to store the data
        logger.info("Attempting to store data in database...")
        db_handler.store_trending_videos(combined_df)
        logger.info("✅ Successfully stored processed data in database")
        
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
            
            if batch_count > 0:
                logger.info("✅ Records were inserted successfully")
            else:
                logger.error(f"❌ No records were inserted")
        
        print("\nData loading successful! Data was inserted into the database.")
        print(f"Check your database with 'python3 check_db.py' to verify.")
        
    except Exception as e:
        logger.error(f"❌ Error during loading: {e}")
        import traceback
        logger.error(traceback.format_exc())
        print("\nLoading failed! See error above.")

if __name__ == "__main__":
    import json  # Import needed for JSON handling
    main()
#!/usr/bin/env python3
"""
Database Structure Analyzer for YouTube Trending Pipeline

This script provides detailed analysis of the SQLite database structure,
comparing it with expected schema and identifying potential issues.
"""

import sqlite3
import os
import argparse
import sys
import yaml
from tabulate import tabulate
import pandas as pd
import pickle
from datetime import datetime

# Add project root to Python path to import modules if needed
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

def analyze_database_structure(db_path='youtube_trending.db', verbose=False):
    """Analyze SQLite database structure in detail"""
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} does not exist!")
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        schema_info = {}
        
        print("\n==== DATABASE STRUCTURE ANALYSIS ====")
        print(f"Database file: {db_path}")
        print(f"File size: {os.path.getsize(db_path):,} bytes")
        print(f"Last modified: {datetime.fromtimestamp(os.path.getmtime(db_path))}")
        print(f"Tables found: {len(tables)}")
        
        for table in tables:
            # Get table schema
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            # Get table row count
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            
            schema_info[table] = {
                'columns': columns,
                'row_count': row_count
            }
            
            # Get foreign keys
            cursor.execute(f"PRAGMA foreign_key_list({table})")
            foreign_keys = cursor.fetchall()
            schema_info[table]['foreign_keys'] = foreign_keys
            
            # Get indexes
            cursor.execute(f"PRAGMA index_list({table})")
            indexes = cursor.fetchall()
            schema_info[table]['indexes'] = indexes
            
            # Check for data
            if row_count > 0 and verbose:
                cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                sample_row = cursor.fetchone()
                schema_info[table]['sample_row'] = sample_row
                
                # Get batch_ids if the table has that column
                if any(col[1] == 'batch_id' for col in columns):
                    cursor.execute(f"SELECT DISTINCT batch_id FROM {table}")
                    batch_ids = [row[0] for row in cursor.fetchall()]
                    schema_info[table]['batch_ids'] = batch_ids
        
        conn.close()
        return schema_info
        
    except Exception as e:
        print(f"Error analyzing database: {str(e)}")
        return None

def get_expected_schema_from_code():
    """Extract expected schema from db_utils.py"""
    try:
        with open('utils/db_utils.py', 'r') as f:
            content = f.read()
        
        # Very basic parsing - in a real implementation you'd want to use
        # AST parsing or more robust methods
        
        expected_schema = {}
        current_table = None
        capture = False
        
        for line in content.split('\n'):
            line = line.strip()
            
            # Detect table definition start
            if 'Table(' in line and '=' in line:
                parts = line.split('=')
                if len(parts) >= 2:
                    table_name_part = parts[0].strip()
                    self_prefix = 'self.'
                    if table_name_part.startswith(self_prefix):
                        current_table = table_name_part[len(self_prefix):]
                        expected_schema[current_table] = {'columns': []}
                        capture = True
            
            # Detect table definition end
            elif capture and ')' in line and current_table:
                capture = False
                current_table = None
            
            # Capture column definitions
            elif capture and current_table and 'Column(' in line:
                # Basic extraction of column name and type
                if ',' in line:
                    parts = line.split(',', 1)
                    col_name = parts[0].strip()
                    if "'" in col_name or '"' in col_name:
                        # Extract the column name
                        start = col_name.find("'") if "'" in col_name else col_name.find('"')
                        end = col_name.rfind("'") if "'" in col_name else col_name.rfind('"')
                        if start != -1 and end != -1 and end > start:
                            col_name = col_name[start+1:end]
                            
                            # Attempt to extract type
                            col_type = None
                            if 'Integer' in line:
                                col_type = 'INTEGER'
                            elif 'String' in line:
                                col_type = 'TEXT'
                            elif 'Float' in line:
                                col_type = 'REAL'
                            elif 'DateTime' in line:
                                col_type = 'TIMESTAMP'
                            elif 'JSON' in line:
                                col_type = 'JSON'
                            
                            expected_schema[current_table]['columns'].append({
                                'name': col_name,
                                'type': col_type
                            })
        
        return expected_schema
        
    except Exception as e:
        print(f"Error getting expected schema: {str(e)}")
        return None

def analyze_pickle_data(pickle_path='data/processed_data.pkl'):
    """Analyze structure of processed data in the pickle file"""
    if not os.path.exists(pickle_path):
        print(f"Warning: Pickle file {pickle_path} does not exist")
        return None
    
    try:
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
        
        if not isinstance(data, dict):
            print(f"Warning: Pickle file does not contain a dictionary")
            return None
        
        print("\n==== PICKLE DATA ANALYSIS ====")
        print(f"File: {pickle_path}")
        print(f"Categories found: {len(data)}")
        
        # Get DataFrame structure for first category
        first_cat = next(iter(data.values())) if data else None
        
        if first_cat is not None and isinstance(first_cat, pd.DataFrame):
            print(f"\nSample DataFrame columns from first category:")
            for col in first_cat.columns:
                print(f"  - {col}: {first_cat[col].dtype}")
            
            # Check for batch_id
            if 'batch_id' in first_cat.columns:
                batch_ids = first_cat['batch_id'].unique()
                print(f"\nBatch IDs found: {', '.join(str(bid) for bid in batch_ids)}")
            
            # Check for duration column specifically
            if 'duration' in first_cat.columns:
                print("\nNote: Found 'duration' column in DataFrame")
                print(f"Sample values: {first_cat['duration'].head(2).values}")
        
        return data
        
    except Exception as e:
        print(f"Error analyzing pickle data: {str(e)}")
        return None

def compare_schema_and_data(db_schema, expected_schema, pickle_data):
    """Compare database schema with expected schema and pickle data"""
    if not db_schema or not expected_schema:
        return
    
    print("\n==== SCHEMA COMPARISON ====")
    
    # Get first DataFrame from pickle data if available
    sample_df = None
    if pickle_data and len(pickle_data) > 0:
        first_cat = next(iter(pickle_data.values()))
        if isinstance(first_cat, pd.DataFrame):
            sample_df = first_cat
    
    for table_name in expected_schema:
        print(f"\nTable: {table_name}")
        
        # Check if table exists in database
        if table_name not in db_schema:
            print(f"  WARNING: Table '{table_name}' defined in code does not exist in database")
            continue
        
        expected_cols = {col['name']: col['type'] for col in expected_schema[table_name]['columns']}
        actual_cols = {col[1]: col[2] for col in db_schema[table_name]['columns']}
        
        # Check for missing columns
        missing_cols = [col for col in expected_cols if col not in actual_cols]
        if missing_cols:
            print(f"  WARNING: Missing columns in database: {', '.join(missing_cols)}")
        
        # Check for columns in DB but not in code definition
        extra_cols = [col for col in actual_cols if col not in expected_cols]
        if extra_cols:
            print(f"  INFO: Extra columns in database (not in code definition): {', '.join(extra_cols)}")
        
        # Check type mismatches
        type_mismatches = []
        for col_name, expected_type in expected_cols.items():
            if col_name in actual_cols:
                actual_type = actual_cols[col_name]
                if expected_type and expected_type != actual_type:
                    type_mismatches.append(f"{col_name} (expected: {expected_type}, actual: {actual_type})")
        
        if type_mismatches:
            print(f"  WARNING: Column type mismatches: {', '.join(type_mismatches)}")
        
        # Compare with pickle DataFrame if available
        if sample_df is not None and table_name == 'trending_videos':
            print("\n  Comparing with DataFrame from pickle:")
            pickle_cols = set(sample_df.columns)
            db_cols = set(actual_cols.keys())
            
            pickle_not_in_db = pickle_cols - db_cols
            if pickle_not_in_db:
                print(f"  WARNING: Columns in DataFrame but not in DB: {', '.join(pickle_not_in_db)}")
                if 'duration' in pickle_not_in_db:
                    print("  CRITICAL: 'duration' column exists in DataFrame but not in database schema")
                    print("           This likely explains the loading failures")
            
            db_not_in_pickle = db_cols - pickle_cols
            if db_not_in_pickle:
                print(f"  INFO: Columns in DB but not in DataFrame: {', '.join(db_not_in_pickle)}")
    
    print("\n==== DIAGNOSIS ====")
    if sample_df is not None and 'duration' in sample_df.columns:
        print("ISSUE DETECTED: The 'duration' column exists in the DataFrame but not in the database schema.")
        print("This mismatch is likely causing the database loading to fail.")
        print("\nSolution options:")
        print("1. Recreate the database to match the current schema:")
        print("   rm youtube_trending.db")
        print("   python -c \"from utils.db_utils import DatabaseHandler; import yaml; config = yaml.safe_load(open('config/config.yaml')); db = DatabaseHandler(config); db.create_tables()\"")
        print("\n2. OR modify the transform.py script to rename or drop the 'duration' column before loading")
        print("3. OR modify db_utils.py to add the 'duration' column to the database schema")

def main():
    parser = argparse.ArgumentParser(description="Database Structure Analyzer")
    parser.add_argument("--db", default="youtube_trending.db", help="Path to SQLite database file")
    parser.add_argument("--pickle", default="data/processed_data.pkl", help="Path to processed data pickle file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show more detailed output")
    args = parser.parse_args()
    
    db_schema = analyze_database_structure(args.db, args.verbose)
    expected_schema = get_expected_schema_from_code()
    pickle_data = analyze_pickle_data(args.pickle)
    
    compare_schema_and_data(db_schema, expected_schema, pickle_data)

if __name__ == "__main__":
    main()
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Airflow DAG for the YouTube Trending Analysis pipeline.
"""

import os
import sys
from datetime import datetime, timedelta
import yaml
import pickle
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add project root to Python path to import modules
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

from scripts.extract import YouTubeExtractor
from scripts.transform import YouTubeTransformer
from scripts.load import YouTubeLoader
from scripts.analyze import YouTubeAnalyzer

# Load configuration
config_path = os.path.join(PROJECT_ROOT, 'config', 'config.yaml')
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'youtube_trending_analysis',
    default_args=default_args,
    description='A pipeline to extract, transform, load, and analyze YouTube trending data',
    schedule_interval=config['airflow']['schedule_interval'],
    start_date=days_ago(1),
    catchup=config['airflow']['catchup'],
    max_active_runs=config['airflow']['max_active_runs'],
    concurrency=config['airflow']['concurrency'],
    tags=['youtube', 'trending', 'data_engineering'],
)

# Define functions for each task
def extract_data(**kwargs):
    """Extract data from YouTube API."""
    # Create temporary directory for intermediate files
    temp_dir = tempfile.mkdtemp()
    
    # Extract data
    extractor = YouTubeExtractor(config)
    raw_data = extractor.get_videos_by_category()
    
    # Save data to temporary file
    output_file = os.path.join(temp_dir, 'raw_data.pkl')
    with open(output_file, 'wb') as f:
        pickle.dump(raw_data, f)
    
    # Push file path to XCom
    kwargs['ti'].xcom_push(key='raw_data_path', value=output_file)
    kwargs['ti'].xcom_push(key='temp_dir', value=temp_dir)
    
    return f"Extracted {sum(len(df) for df in raw_data.values())} videos from YouTube API"

def transform_data(**kwargs):
    """Transform the extracted data."""
    # Get raw data file path from XCom
    ti = kwargs['ti']
    raw_data_path = ti.xcom_pull(task_ids='extract_data', key='raw_data_path')
    temp_dir = ti.xcom_pull(task_ids='extract_data', key='temp_dir')
    
    # Load raw data
    with open(raw_data_path, 'rb') as f:
        raw_data = pickle.load(f)
    
    # Transform data
    transformer = YouTubeTransformer(config)
    processed_data = transformer.transform_all_categories(raw_data)
    
    # Save data to temporary file
    output_file = os.path.join(temp_dir, 'processed_data.pkl')
    with open(output_file, 'wb') as f:
        pickle.dump(processed_data, f)
    
    # Push file path to XCom
    ti.xcom_push(key='processed_data_path', value=output_file)
    
    return f"Transformed {sum(len(df) for df in processed_data.values())} videos"

def load_data(**kwargs):
    """Load the processed data to storage systems."""
    # Get data file paths from XCom
    ti = kwargs['ti']
    raw_data_path = ti.xcom_pull(task_ids='extract_data', key='raw_data_path')
    processed_data_path = ti.xcom_pull(task_ids='transform_data', key='processed_data_path')
    temp_dir = ti.xcom_pull(task_ids='extract_data', key='temp_dir')
    
    # Load data
    with open(raw_data_path, 'rb') as f:
        raw_data = pickle.load(f)
    
    with open(processed_data_path, 'rb') as f:
        processed_data = pickle.load(f)
    
    # Load to storage systems
    loader = YouTubeLoader(config)
    results = loader.load_data(raw_data, processed_data)
    
    # Save results to temporary file
    output_file = os.path.join(temp_dir, 'load_results.pkl')
    with open(output_file, 'wb') as f:
        pickle.dump(results, f)
    
    # Push file path to XCom
    ti.xcom_push(key='load_results_path', value=output_file)
    ti.xcom_push(key='batch_id', value=results['batch_id'])
    
    return f"Loaded data to storage systems with batch ID: {results['batch_id']}"

def analyze_data(**kwargs):
    """Analyze the processed data."""
    # Get data file path from XCom
    ti = kwargs['ti']
    processed_data_path = ti.xcom_pull(task_ids='transform_data', key='processed_data_path')
    temp_dir = ti.xcom_pull(task_ids='extract_data', key='temp_dir')
    
    # Load data
    with open(processed_data_path, 'rb') as f:
        processed_data = pickle.load(f)
    
    # Analyze data
    analyzer = YouTubeAnalyzer(config)
    results = analyzer.run_analysis(processed_data)
    
    # Save results to temporary file
    output_file = os.path.join(temp_dir, 'analysis_results.pkl')
    with open(output_file, 'wb') as f:
        pickle.dump(results, f)
    
    # Push file path to XCom
    ti.xcom_push(key='analysis_results_path', value=output_file)
    
    return f"Analyzed trending data and saved results to S3: {results.get('s3_uri', 'N/A')}"

def update_dashboard(**kwargs):
    """Update the dashboard with the latest analysis results."""
    # Get batch ID from XCom
    ti = kwargs['ti']
    batch_id = ti.xcom_pull(task_ids='load_data', key='batch_id')
    analysis_results_path = ti.xcom_pull(task_ids='analyze_data', key='analysis_results_path')
    
    # Load analysis results
    with open(analysis_results_path, 'rb') as f:
        analysis_results = pickle.load(f)
    
    # Update dashboard assets in S3
    from utils.s3_utils import S3Handler
    s3_handler = S3Handler(config)
    
    # Generate dashboard HTML
    dashboard_html = generate_dashboard_html(analysis_results, batch_id)
    
    # Upload to S3
    dashboard_uri = s3_handler.upload_dashboard_assets(
        'index.html', 
        dashboard_html, 
        'text/html'
    )
    
    return f"Updated dashboard with latest analysis results: {dashboard_uri}"

def cleanup_temp_files(**kwargs):
    """Clean up temporary files."""
    import shutil
    
    # Get temp directory from XCom
    ti = kwargs['ti']
    temp_dir = ti.xcom_pull(task_ids='extract_data', key='temp_dir')
    
    # Remove temp directory
    if temp_dir and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    
    return f"Cleaned up temporary files in {temp_dir}"

def generate_dashboard_html(analysis_results, batch_id):
    """Generate HTML for the dashboard."""
    # Simple template for demonstration
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>YouTube Trending Analysis Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        .chart-container {{
            margin-bottom: 30px;
        }}
        .card {{
            margin-bottom: 20px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }}
        .card-header {{
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="container-fluid mt-3">
        <div class="row">
            <div class="col-12">
                <h1>YouTube Trending Analysis Dashboard</h1>
                <p class="lead">Batch ID: {batch_id} | Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </div>
        
        <div class="row mt-4">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">Top Categories</div>
                    <div class="card-body">
                        <div class="chart-container">
                            <img src="/api/placeholder/600/400" alt="Top Categories" class="img-fluid" />
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">Video Length vs. Views</div>
                    <div class="card-body">
                        <div class="chart-container">
                            <img src="/api/placeholder/600/400" alt="Video Length vs Views" class="img-fluid" />
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row">
            <div class="col-md-12">
                <div class="card">
                    <div class="card-header">Top Trending Videos</div>
                    <div class="card-body">
                        <div class="table-responsive">
                            <table class="table table-striped">
                                <thead>
                                    <tr>
                                        <th>Title</th>
                                        <th>Channel</th>
                                        <th>Category</th>
                                        <th>Views</th>
                                        <th>Likes</th>
                                        <th>Views/Hour</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <!-- Dynamically generated table rows would go here -->
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""
    return html

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

analyze_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    provide_context=True,
    dag=dag,
)

update_dashboard_task = PythonOperator(
    task_id='update_dashboard',
    python_callable=update_dashboard,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
extract_task >> transform_task >> load_task >> analyze_task >> update_dashboard_task >> cleanup_task
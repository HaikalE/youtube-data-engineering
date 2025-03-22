# YouTube Trending Video Analysis

A comprehensive data engineering project for extracting, processing, analyzing and visualizing YouTube trending videos data.

![Dashboard Preview](dashboard/assets/dashboard-preview.png)

## Project Overview

This project creates an end-to-end data pipeline to process YouTube trending videos data, store it in multiple storage systems, and provide insights through analytics and visualization. The pipeline is fully automated using Apache Airflow and includes integration with Amazon S3 for cloud storage.

### Features

- **ETL Pipeline**: Extract data from YouTube API, transform it to derive useful metrics, and load it into PostgreSQL and Amazon S3
- **Automated Workflow**: Apache Airflow DAG for scheduled data collection and processing
- **Comprehensive Analysis**: Identify trends, patterns, and insights from trending videos
- **Interactive Dashboard**: Visualize data using Plotly Dash with filtering capabilities
- **Cloud Storage**: Amazon S3 integration for reliable data storage and retrieval
- **Modular Design**: Well-structured, maintainable, and extensible codebase

## Architecture

![Architecture Diagram](dashboard/assets/architecture.png)

### Components

1. **Data Sources**:
   - YouTube Data API v3

2. **Data Processing**:
   - Python scripts for extraction, transformation, loading, and analysis
   - Apache Airflow for workflow orchestration

3. **Storage**:
   - PostgreSQL database for structured data and queries
   - Amazon S3 for raw, processed, and analysis data

4. **Visualization**:
   - Plotly Dash for interactive dashboard
   - Static visualizations generated using Matplotlib/Seaborn

## Project Structure

```
.
├── README.md                # Project documentation
├── requirements.txt         # Python dependencies
├── config/                  # Configuration files
│   └── config.yaml          # Main configuration
├── dags/                    # Airflow DAGs
│   └── youtube_trending_dag.py  # Main pipeline DAG
├── scripts/                 # Core processing scripts
│   ├── extract.py           # YouTube API data extraction
│   ├── transform.py         # Data transformation and feature engineering
│   ├── load.py              # Data loading to storage systems
│   └── analyze.py           # Data analysis and visualization
├── utils/                   # Utility modules
│   ├── s3_utils.py          # Amazon S3 operations
│   └── db_utils.py          # Database operations
├── dashboard/               # Dash web application
│   ├── app.py               # Dashboard application
│   ├── assets/              # Static assets
│   └── templates/           # HTML templates
├── notebooks/               # Jupyter notebooks for exploration
└── tests/                   # Unit and integration tests
```

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL or SQLite
- Apache Airflow
- AWS account (for S3 integration)
- YouTube Data API key

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/youtube-trending-analysis.git
   cd youtube-trending-analysis
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```bash
   export YOUTUBE_API_KEY="your_youtube_api_key"
   export AWS_ACCESS_KEY_ID="your_aws_access_key"
   export AWS_SECRET_ACCESS_KEY="your_aws_secret_key"
   export DB_PASSWORD="your_database_password"
   ```

4. Update configuration:
   - Edit `config/config.yaml` with your specific configuration

### Running the Pipeline

#### Using Airflow

1. Start Airflow scheduler and webserver:
   ```bash
   airflow scheduler
   airflow webserver
   ```

2. Access the Airflow UI at `http://localhost:8080` and enable the DAG `youtube_trending_analysis`

#### Manual Execution

1. Run the extraction script:
   ```bash
   python -m scripts.extract config/config.yaml
   ```

2. Run the transformation script:
   ```bash
   python -m scripts.transform config/config.yaml raw_data.pkl
   ```

3. Run the loading script:
   ```bash
   python -m scripts.load config/config.yaml raw_data.pkl processed_data.pkl
   ```

4. Run the analysis script:
   ```bash
   python -m scripts.analyze config/config.yaml processed_data.pkl
   ```

### Running the Dashboard

Start the Dash application:
```bash
python -m dashboard.app
```

Access the dashboard at `http://localhost:8050`

## Analysis Features

The project provides insights on:

1. **Category Trends**: Which categories dominate trending videos?
2. **Top Videos**: Videos with highest engagement and growth rates
3. **Channel Analysis**: Most successful channels in trending sections
4. **Content Patterns**: Optimal video length, publishing time, etc.
5. **Engagement Metrics**: View-to-like ratios, comment rates, views per hour
6. **Tag & Hashtag Analysis**: Most effective tags and hashtags

## Dashboard Components

- **Category Distribution**: Breakdown of videos by category
- **Time Trends**: Trending patterns over time
- **Top Videos Table**: Sortable list of top-performing videos
- **Engagement Charts**: Visualization of key engagement metrics
- **Channel Leaderboard**: Top channels by various metrics
- **Content Feature Analysis**: Impact of video length, title, etc.

## Future Enhancements

- Natural Language Processing on video titles and descriptions
- Sentiment analysis of comments
- Machine learning models to predict trending potential
- Competitor analysis for channels
- Regional trend comparison

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

#### License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- YouTube Data API documentation
- Apache Airflow community
- AWS SDK for Python (Boto3)

## Technical Details

### Data Pipeline Architecture

The data pipeline consists of four main stages, each implemented as a separate task in the Airflow DAG:

1. **Extract**: Data is extracted from the YouTube API using the `GoogleApiClient` library. The API is queried for trending videos across different categories, and the results are stored temporarily.

2. **Transform**: Raw data undergoes transformation to:
   - Normalize timestamps
   - Calculate derived metrics like engagement ratios
   - Extract features from text (titles, descriptions)
   - Categorize videos by length, publishing patterns, etc.

3. **Load**: Processed data is loaded into two storage systems:
   - **PostgreSQL**: Structured data with proper indexing for efficient querying
   - **Amazon S3**: Raw, processed, and analysis results stored in designated prefixes

4. **Analyze**: The processed data is analyzed to identify trends and patterns, and the results are:
   - Stored in the database for historical tracking
   - Uploaded to S3 for archiving
   - Used to update the dashboard visualizations

### YouTube API Integration

The project uses YouTube Data API v3 to fetch trending videos information. Key endpoints used:

- `videos.list` with `chart=mostPopular` parameter to get trending videos
- Region code and category filters to segment data

API responses are processed to extract:
- Video metadata (title, channel, category, etc.)
- Performance metrics (views, likes, comments)
- Publishing information (date, time)

### Data Modeling

The database schema consists of several tables:

1. **trending_videos**: Main table storing video-level data
   - Primary key: auto-incrementing ID
   - Indexes: video_id, channel_id, category_id, batch_id
   
2. **channel_stats**: Aggregated channel performance data
   - Computed from trending_videos during analysis
   
3. **trends_summary**: Summary statistics by category
   - Updated with each data collection cycle
   
4. **hashtags**: Trending hashtags and their frequency
   - Linked to categories for segmented analysis

### AWS S3 Integration

The S3 storage is organized into prefixes:

- **raw/**: JSON responses from the YouTube API
- **processed/**: Transformed and enriched data
- **analysis/**: Analysis results and visualizations
- **dashboard/**: Static assets for the dashboard

Files are stored in parquet format for efficient storage and retrieval, with CSV exports available for compatibility.

### Dashboard Technology

The dashboard is built with Plotly Dash, providing:

- Interactive data exploration
- Filtering and sorting capabilities
- Dynamic chart generation
- Responsive design for mobile and desktop

## Performance Considerations

- **API Rate Limiting**: YouTube API has a daily quota (10,000 units). The pipeline is designed to work within these constraints.
- **Database Indexing**: Strategic indexes on frequently queried columns
- **Batch Processing**: Data is processed in batches identified by timestamp
- **Incremental Updates**: Only new trending videos are analyzed in depth
- **Caching**: Dashboard implements caching for frequently accessed visualizations

## Deployment Options

1. **Local Development**:
   - Run all components on a local machine
   - SQLite for database
   - LocalStack for simulating AWS services

2. **Cloud Deployment**:
   - AWS EC2 for compute
   - AWS RDS for PostgreSQL
   - Native S3 for storage
   - AWS CloudWatch for monitoring

3. **Container-Based**:
   - Docker containers for each component
   - Docker Compose for local orchestration
   - Kubernetes for production scaling

## Monitoring and Maintenance

- Airflow provides built-in monitoring for pipeline runs
- Logging is implemented at multiple levels for troubleshooting
- S3 lifecycle policies for managing storage costs
- Database maintenance scripts for optimization

## Contact

For questions or feedback about this project, please reach out to:
- Email: your.email@example.com
- GitHub: [Your GitHub Profile](https://github.com/yourusername)
- LinkedIn: [Your LinkedIn Profile](https://linkedin.com/in/yourusername)
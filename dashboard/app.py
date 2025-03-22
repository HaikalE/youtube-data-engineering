#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Dash application for the YouTube Trending Analysis dashboard.
"""

import os
import sys
import yaml
import pandas as pd
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State

# Add project root to Python path to import modules
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

from utils.db_utils import DatabaseHandler
from utils.s3_utils import S3Handler

# Load configuration
config_path = os.path.join(PROJECT_ROOT, 'config', 'config.yaml')
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# Create database handler
db_handler = DatabaseHandler(config)

# Create S3 handler
s3_handler = S3Handler(config)

# Create the Dash app
app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    external_stylesheets=[
        "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"
    ]
)
app.title = "YouTube Trending Analysis Dashboard"
server = app.server

# Helper functions
def get_latest_batch_id():
    """Get the latest batch ID from the database."""
    with db_handler.engine.connect() as conn:
        result = conn.execute("SELECT DISTINCT batch_id FROM trending_videos ORDER BY batch_id DESC LIMIT 1")
        latest_batch = result.fetchone()
        return latest_batch[0] if latest_batch else None

def get_batch_timestamp(batch_id):
    """Get the timestamp for a batch ID."""
    with db_handler.engine.connect() as conn:
        result = conn.execute(
            f"SELECT extracted_at FROM trending_videos WHERE batch_id = '{batch_id}' LIMIT 1"
        )
        timestamp = result.fetchone()
        return timestamp[0] if timestamp else None

def get_available_batches():
    """Get all available batch IDs from the database."""
    with db_handler.engine.connect() as conn:
        result = conn.execute("SELECT DISTINCT batch_id FROM trending_videos ORDER BY batch_id DESC")
        batches = [row[0] for row in result.fetchall()]
        return batches

def get_category_data(batch_id):
    """Get category data for a specific batch."""
    with db_handler.engine.connect() as conn:
        df = pd.read_sql(
            f"""
            SELECT category_id, category_name, COUNT(*) as video_count, 
                   AVG(view_count) as avg_views, AVG(like_count) as avg_likes,
                   AVG(comment_count) as avg_comments
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
            GROUP BY category_id, category_name
            ORDER BY video_count DESC
            """,
            conn
        )
        return df

def get_top_videos(batch_id, limit=10, sort_by='views_per_hour'):
    """Get top videos for a specific batch."""
    with db_handler.engine.connect() as conn:
        df = pd.read_sql(
            f"""
            SELECT video_id, title, channel_title, category_name, view_count, 
                   like_count, comment_count, views_per_hour, like_view_ratio
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
            ORDER BY {sort_by} DESC
            LIMIT {limit}
            """,
            conn
        )
        return df

def get_top_channels(batch_id, limit=10):
    """Get top channels for a specific batch."""
    with db_handler.engine.connect() as conn:
        df = pd.read_sql(
            f"""
            SELECT channel_id, channel_title, COUNT(*) as video_count,
                   AVG(view_count) as avg_views, AVG(like_count) as avg_likes
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
            GROUP BY channel_id, channel_title
            ORDER BY video_count DESC
            LIMIT {limit}
            """,
            conn
        )
        return df

def get_duration_stats(batch_id):
    """Get video duration statistics for a specific batch."""
    with db_handler.engine.connect() as conn:
        df = pd.read_sql(
            f"""
            SELECT length_category, COUNT(*) as video_count,
                   AVG(view_count) as avg_views, AVG(like_view_ratio) as avg_like_ratio
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
            GROUP BY length_category
            ORDER BY CASE 
                WHEN length_category = '< 1 min' THEN 1
                WHEN length_category = '1-5 min' THEN 2
                WHEN length_category = '5-10 min' THEN 3
                WHEN length_category = '10-20 min' THEN 4
                WHEN length_category = '> 20 min' THEN 5
            END
            """,
            conn
        )
        return df

def get_top_hashtags(batch_id, limit=20):
    """Get top hashtags for a specific batch."""
    with db_handler.engine.connect() as conn:
        df = pd.read_sql(
            f"""
            SELECT hashtag, COUNT as count
            FROM hashtags
            WHERE batch_id = '{batch_id}'
            ORDER BY count DESC
            LIMIT {limit}
            """,
            conn
        )
        return df

# Layout
app.layout = html.Div([
    # Header
    html.Div([
        html.Div([
            html.H1("YouTube Trending Analysis Dashboard", className="display-4"),
            html.P("Interactive analytics for YouTube trending videos", className="lead"),
        ], className="col-md-8"),
        
        html.Div([
            html.Div([
                html.Label("Select Batch:", className="form-label"),
                dcc.Dropdown(
                    id='batch-dropdown',
                    options=[],
                    value=None,
                    clearable=False,
                    className="form-select"
                ),
            ], className="mb-3"),
            html.Div(id='batch-info', className="small text-muted")
        ], className="col-md-4"),
    ], className="row mb-4 align-items-center"),
    
    # Stats Cards
    html.Div([
        html.Div([
            html.Div([
                html.H5("Total Videos", className="card-title"),
                html.H2(id="total-videos", className="display-6 fw-bold"),
            ], className="card-body text-center")
        ], className="card bg-light"),
    ], className="col-md-3"),
        
    html.Div([
        html.Div([
            html.Div([
                html.H5("Avg. Views", className="card-title"),
                html.H2(id="avg-views", className="display-6 fw-bold"),
            ], className="card-body text-center")
        ], className="card bg-light"),
    ], className="col-md-3"),
        
    html.Div([
        html.Div([
            html.Div([
                html.H5("Avg. Likes", className="card-title"),
                html.H2(id="avg-likes", className="display-6 fw-bold"),
            ], className="card-body text-center")
        ], className="card bg-light"),
    ], className="col-md-3"),
        
    html.Div([
        html.Div([
            html.Div([
                html.H5("Avg. Comments", className="card-title"),
                html.H2(id="avg-comments", className="display-6 fw-bold"),
            ], className="card-body text-center")
        ], className="card bg-light"),
    ], className="col-md-3"),
    
    # Main Content
    html.Div([
        # Categories
        html.Div([
            html.Div([
                html.Div([
                    html.H4("Video Distribution by Category", className="card-title"),
                    dcc.Graph(id="category-chart")
                ], className="card-body")
            ], className="card mb-4"),
            
            # Duration Stats
            html.Div([
                html.Div([
                    html.H4("Video Metrics by Duration", className="card-title"),
                    dcc.Graph(id="duration-chart")
                ], className="card-body")
            ], className="card mb-4"),
            
            # Top Hashtags
            html.Div([
                html.Div([
                    html.H4("Top Hashtags", className="card-title"),
                    dcc.Graph(id="hashtags-chart")
                ], className="card-body")
            ], className="card mb-4"),
        ], className="col-md-6"),
        
        # Top Videos & Channels
        html.Div([
            # Top Videos Table
            html.Div([
                html.Div([
                    html.H4("Top Trending Videos", className="card-title"),
                    html.Div([
                        html.Label("Sort by:"),
                        dcc.RadioItems(
                            id='video-sort-options',
                            options=[
                                {'label': 'Views per Hour', 'value': 'views_per_hour'},
                                {'label': 'Total Views', 'value': 'view_count'},
                                {'label': 'Like Ratio', 'value': 'like_view_ratio'}
                            ],
                            value='views_per_hour',
                            inline=True,
                            className="mb-2"
                        ),
                    ]),
                    dash_table.DataTable(
                        id='top-videos-table',
                        columns=[
                            {"name": "Title", "id": "title"},
                            {"name": "Channel", "id": "channel_title"},
                            {"name": "Category", "id": "category_name"},
                            {"name": "Views", "id": "view_count", "type": "numeric", "format": {"specifier": ","}},
                            {"name": "Likes", "id": "like_count", "type": "numeric", "format": {"specifier": ","}},
                            {"name": "Views/Hour", "id": "views_per_hour", "type": "numeric", "format": {"specifier": ",.1f"}}
                        ],
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                            'maxWidth': 0,
                        },
                        style_header={
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight': 'bold'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }
                        ],
                        page_size=5
                    )
                ], className="card-body")
            ], className="card mb-4"),
            
            # Top Channels Table
            html.Div([
                html.Div([
                    html.H4("Top Channels", className="card-title"),
                    dash_table.DataTable(
                        id='top-channels-table',
                        columns=[
                            {"name": "Channel", "id": "channel_title"},
                            {"name": "Videos", "id": "video_count", "type": "numeric"},
                            {"name": "Avg. Views", "id": "avg_views", "type": "numeric", "format": {"specifier": ",.1f"}},
                            {"name": "Avg. Likes", "id": "avg_likes", "type": "numeric", "format": {"specifier": ",.1f"}}
                        ],
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                            'maxWidth': 0,
                        },
                        style_header={
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight': 'bold'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }
                        ],
                        page_size=5
                    )
                ], className="card-body")
            ], className="card")
        ], className="col-md-6")
    ], className="row"),
    
    # Footer
    html.Footer([
        html.P("YouTube Trending Analysis Project | Data Engineering Portfolio", className="text-center text-muted")
    ], className="mt-4 pt-3 border-top")
], className="container-fluid p-4")

# Callbacks
@app.callback(
    Output('batch-dropdown', 'options'),
    Output('batch-dropdown', 'value'),
    Input('batch-dropdown', 'options')  # This is just a dummy input to trigger on load
)
def update_batch_dropdown(existing_options):
    """Update the batch dropdown options and select the latest batch."""
    batches = get_available_batches()
    options = [{'label': batch, 'value': batch} for batch in batches]
    return options, batches[0] if batches else None

@app.callback(
    Output('batch-info', 'children'),
    Input('batch-dropdown', 'value')
)
def update_batch_info(batch_id):
    """Update the batch information text."""
    if not batch_id:
        return "No data available"
    
    timestamp = get_batch_timestamp(batch_id)
    if timestamp:
        return f"Data extracted on: {timestamp}"
    return "Timestamp not available"

@app.callback(
    [
        Output('total-videos', 'children'),
        Output('avg-views', 'children'),
        Output('avg-likes', 'children'),
        Output('avg-comments', 'children'),
        Output('category-chart', 'figure'),
        Output('duration-chart', 'figure'),
        Output('hashtags-chart', 'figure'),
        Output('top-videos-table', 'data'),
        Output('top-channels-table', 'data')
    ],
    [
        Input('batch-dropdown', 'value'),
        Input('video-sort-options', 'value')
    ]
)
def update_dashboard(batch_id, video_sort_by):
    """Update all dashboard components based on the selected batch."""
    if not batch_id:
        # Return empty/default values if no batch is selected
        empty_fig = go.Figure()
        empty_fig.update_layout(
            xaxis={"visible": False},
            yaxis={"visible": False},
            annotations=[{
                "text": "No data available",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return "0", "0", "0", "0", empty_fig, empty_fig, empty_fig, [], []
    
    # Get data for the selected batch
    category_data = get_category_data(batch_id)
    duration_data = get_duration_stats(batch_id)
    hashtag_data = get_top_hashtags(batch_id)
    top_videos = get_top_videos(batch_id, sort_by=video_sort_by)
    top_channels = get_top_channels(batch_id)
    
    # Calculate summary statistics
    total_videos = category_data['video_count'].sum()
    avg_views = f"{int(category_data['avg_views'].mean()):,}"
    avg_likes = f"{int(category_data['avg_likes'].mean()):,}"
    avg_comments = f"{int(category_data['avg_comments'].mean()):,}"
    
    # Create category chart
    category_fig = px.bar(
        category_data,
        x='video_count',
        y='category_name',
        orientation='h',
        color='avg_views',
        color_continuous_scale='viridis',
        labels={
            'video_count': 'Number of Videos',
            'category_name': 'Category',
            'avg_views': 'Average Views'
        },
        title='Video Distribution by Category'
    )
    category_fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    # Create duration chart
    duration_fig = px.bar(
        duration_data,
        x='length_category',
        y=['video_count', 'avg_views', 'avg_like_ratio'],
        barmode='group',
        labels={
            'length_category': 'Video Length',
            'value': 'Value',
            'variable': 'Metric'
        },
        title='Video Metrics by Duration'
    )
    
    # Create hashtags chart
    hashtag_fig = px.bar(
        hashtag_data,
        x='count',
        y='hashtag',
        orientation='h',
        labels={
            'count': 'Occurrences',
            'hashtag': 'Hashtag'
        },
        title='Top Hashtags'
    )
    hashtag_fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    return (
        f"{total_videos:,}",
        avg_views,
        avg_likes,
        avg_comments,
        category_fig,
        duration_fig,
        hashtag_fig,
        top_videos.to_dict('records'),
        top_channels.to_dict('records')
    )

if __name__ == "__main__":
    app.run_server(
        host=config['dashboard']['host'],
        port=config['dashboard']['port'],
        debug=config['dashboard']['debug']
    )
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Dash application for the YouTube Trending Analysis dashboard.
Enhanced with professional visualizations and improved UX.
"""

import os
import sys
import yaml
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash.long_callback import DiskcacheLongCallbackManager
import diskcache

# Add project root to Python path to import modules
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

from utils.db_utils import DatabaseHandler
from utils.s3_utils import S3Handler

# Setup caching for performance
cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(cache)

# Load configuration
config_path = os.path.join(PROJECT_ROOT, 'config', 'config.yaml')
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# Create database handler
db_handler = DatabaseHandler(config)

# Create S3 handler
s3_handler = S3Handler(config)

# Create the Dash app with caching
app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    external_stylesheets=[
        "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css",
        "https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap"
    ],
    long_callback_manager=long_callback_manager,
    suppress_callback_exceptions=True  # Added to fix callback exceptions
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

def get_available_categories():
    """Get all available categories from the database."""
    with db_handler.engine.connect() as conn:
        result = conn.execute("SELECT DISTINCT category_id, category_name FROM trending_videos ORDER BY category_name")
        categories = {row[0]: row[1] for row in result.fetchall()}
        return categories

def get_category_data(batch_id, category_ids=None):
    """Get category data for a specific batch."""
    with db_handler.engine.connect() as conn:
        query = f"""
            SELECT category_id, category_name, COUNT(*) as video_count, 
                   AVG(view_count) as avg_views, AVG(like_count) as avg_likes,
                   AVG(comment_count) as avg_comments, AVG(views_per_hour) as avg_views_per_hour,
                   AVG(like_view_ratio) as avg_like_ratio
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
        """
        
        if category_ids:
            category_list = ", ".join([str(cat_id) for cat_id in category_ids])
            query += f" AND category_id IN ({category_list})"
            
        query += " GROUP BY category_id, category_name ORDER BY video_count DESC"
        
        df = pd.read_sql(query, conn)
        return df

def get_top_videos(batch_id, limit=10, sort_by='views_per_hour', category_ids=None, min_views=None):
    """Get top videos for a specific batch."""
    with db_handler.engine.connect() as conn:
        query = f"""
            SELECT video_id, title, channel_title, category_name, view_count, 
                   like_count, comment_count, views_per_hour, like_view_ratio,
                   publish_time, duration_seconds
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
        """
        
        if category_ids:
            category_list = ", ".join([str(cat_id) for cat_id in category_ids])
            query += f" AND category_id IN ({category_list})"
            
        if min_views:
            query += f" AND view_count >= {min_views}"
            
        query += f" ORDER BY {sort_by} DESC LIMIT {limit}"
        
        df = pd.read_sql(query, conn)
        return df

def get_top_channels(batch_id, limit=10, category_ids=None):
    """Get top channels for a specific batch."""
    with db_handler.engine.connect() as conn:
        query = f"""
            SELECT channel_id, channel_title, COUNT(*) as video_count,
                   AVG(view_count) as avg_views, AVG(like_count) as avg_likes,
                   AVG(views_per_hour) as avg_views_per_hour
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
        """
        
        if category_ids:
            category_list = ", ".join([str(cat_id) for cat_id in category_ids])
            query += f" AND category_id IN ({category_list})"
            
        query += f" GROUP BY channel_id, channel_title ORDER BY video_count DESC LIMIT {limit}"
        
        df = pd.read_sql(query, conn)
        return df

def get_duration_stats(batch_id, category_ids=None):
    """Get video duration statistics for a specific batch."""
    with db_handler.engine.connect() as conn:
        query = f"""
            SELECT length_category, COUNT(*) as video_count,
                   AVG(view_count) as avg_views, AVG(like_view_ratio) as avg_like_ratio,
                   AVG(views_per_hour) as avg_views_per_hour
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
        """
        
        if category_ids:
            category_list = ", ".join([str(cat_id) for cat_id in category_ids])
            query += f" AND category_id IN ({category_list})"
            
        query += """
            GROUP BY length_category
            ORDER BY CASE 
                WHEN length_category = '< 1 min' THEN 1
                WHEN length_category = '1-5 min' THEN 2
                WHEN length_category = '5-10 min' THEN 3
                WHEN length_category = '10-20 min' THEN 4
                WHEN length_category = '> 20 min' THEN 5
            END
        """
        
        df = pd.read_sql(query, conn)
        return df

def get_top_hashtags(batch_id, limit=20, category_ids=None):
    """Get top hashtags for a specific batch."""
    with db_handler.engine.connect() as conn:
        query = f"""
            SELECT hashtag, COUNT as count, category_name
            FROM hashtags
            WHERE batch_id = '{batch_id}'
        """
        
        if category_ids:
            category_list = ", ".join([str(cat_id) for cat_id in category_ids])
            query += f" AND category_id IN ({category_list})"
            
        query += f" ORDER BY count DESC LIMIT {limit}"
        
        df = pd.read_sql(query, conn)
        return df

def get_time_of_day_stats(batch_id, category_ids=None):
    """Get metrics by time of day for a specific batch."""
    with db_handler.engine.connect() as conn:
        query = f"""
            SELECT 
                CAST(strftime('%H', publish_time) AS INTEGER) as hour_of_day,
                COUNT(*) as video_count,
                AVG(view_count) as avg_views,
                AVG(views_per_hour) as avg_views_per_hour
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
        """
        
        if category_ids:
            category_list = ", ".join([str(cat_id) for cat_id in category_ids])
            query += f" AND category_id IN ({category_list})"
            
        query += " GROUP BY hour_of_day ORDER BY hour_of_day"
        
        df = pd.read_sql(query, conn)
        return df

def get_dashboard_summary(batch_id, category_ids=None):
    """Get summary metrics for dashboard."""
    with db_handler.engine.connect() as conn:
        query = f"""
            SELECT 
                COUNT(*) as total_videos,
                AVG(view_count) as avg_views,
                AVG(like_count) as avg_likes,
                AVG(comment_count) as avg_comments,
                AVG(views_per_hour) as avg_views_per_hour,
                AVG(like_view_ratio) as avg_like_ratio,
                MAX(view_count) as max_views,
                MAX(like_count) as max_likes
            FROM trending_videos
            WHERE batch_id = '{batch_id}'
        """
        
        if category_ids:
            category_list = ", ".join([str(cat_id) for cat_id in category_ids])
            query += f" AND category_id IN ({category_list})"
            
        df = pd.read_sql(query, conn)
        return df.iloc[0] if not df.empty else None

def get_trending_growth(current_batch_id, category_ids=None):
    """Compare current batch with previous batch."""
    batches = get_available_batches()
    if len(batches) < 2 or batches[0] != current_batch_id:
        return None
    
    previous_batch_id = batches[1]
    
    # Get current batch summary
    current_summary = get_dashboard_summary(current_batch_id, category_ids)
    
    # Get previous batch summary
    previous_summary = get_dashboard_summary(previous_batch_id, category_ids)
    
    if current_summary is None or previous_summary is None:
        return None
    
    # Calculate growth percentages
    growth = {
        'views_growth': ((current_summary['avg_views'] / previous_summary['avg_views']) - 1) * 100 
                        if previous_summary['avg_views'] > 0 else 0,
        'likes_growth': ((current_summary['avg_likes'] / previous_summary['avg_likes']) - 1) * 100 
                        if previous_summary['avg_likes'] > 0 else 0,
        'comments_growth': ((current_summary['avg_comments'] / previous_summary['avg_comments']) - 1) * 100 
                          if previous_summary['avg_comments'] > 0 else 0,
        'views_per_hour_growth': ((current_summary['avg_views_per_hour'] / previous_summary['avg_views_per_hour']) - 1) * 100 
                                if previous_summary['avg_views_per_hour'] > 0 else 0
    }
    
    return growth

def generate_insights(batch_id, category_ids=None):
    """Generate automated insights from the data."""
    insights = []
    
    # Get category data
    category_data = get_category_data(batch_id, category_ids)
    
    if not category_data.empty:
        # Top category by video count
        top_category = category_data.iloc[0]
        insights.append({
            "title": "Most Popular Category",
            "value": top_category['category_name'],
            "metric": f"{int(top_category['video_count'])} videos",
            "icon": "trophy"
        })
        
        # Best engagement category
        best_engagement = category_data.sort_values('avg_like_ratio', ascending=False).iloc[0]
        insights.append({
            "title": "Best Engagement",
            "value": best_engagement['category_name'],
            "metric": f"{best_engagement['avg_like_ratio']:.1f}% like rate",
            "icon": "heart"
        })
        
        # Fastest growing (views per hour)
        fastest_growing = category_data.sort_values('avg_views_per_hour', ascending=False).iloc[0]
        insights.append({
            "title": "Fastest Growing",
            "value": fastest_growing['category_name'],
            "metric": f"{int(fastest_growing['avg_views_per_hour'])} views/hr",
            "icon": "trending-up"
        })
    
    # Growth compared to previous batch
    growth = get_trending_growth(batch_id, category_ids)
    if growth:
        # Overall trend
        avg_growth = np.mean([v for v in growth.values()])
        trend = "up" if avg_growth > 0 else "down"
        insights.append({
            "title": "Overall Trend",
            "value": "Increasing" if trend == "up" else "Decreasing",
            "metric": f"{abs(avg_growth):.1f}% {trend}",
            "icon": f"trending-{trend}"
        })
    
    return insights

# Create visualization functions
def create_category_chart(df):
    """Create an enhanced category distribution chart."""
    if df.empty:
        return empty_chart("No category data available")
    
    # Custom color scheme
    fig = px.bar(
        df,
        x='video_count',
        y='category_name',
        orientation='h',
        color='avg_views',
        color_continuous_scale=px.colors.sequential.Reds,  # YouTube-themed color
        labels={
            'video_count': 'Number of Videos',
            'category_name': 'Category',
            'avg_views': 'Average Views'
        },
        hover_data=['avg_views', 'avg_likes', 'avg_comments'],
        title='Video Distribution by Category'
    )
    
    # Enhanced styling
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font={'family': 'Roboto, Arial, sans-serif'},
        margin=dict(l=20, r=20, t=40, b=20),
        hoverlabel=dict(bgcolor='white', font_size=14, font_family='Roboto'),
        coloraxis_colorbar=dict(title='Avg Views'),
        height=400
    )
    
    return fig

def create_duration_chart(df):
    """Create an enhanced video duration impact chart."""
    if df.empty:
        return empty_chart("No duration data available")
    
    # Create subplot with two y-axes
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Bar chart for video count
    fig.add_trace(
        go.Bar(
            x=df['length_category'],
            y=df['video_count'],
            name='Video Count',
            marker_color='#FF0000',
            opacity=0.7
        )
    )
    
    # Line chart for average views per hour
    fig.add_trace(
        go.Scatter(
            x=df['length_category'],
            y=df['avg_views_per_hour'],
            name='Avg Views/Hour',
            mode='lines+markers',
            marker=dict(size=8, color='#4285F4'),
            line=dict(width=3, color='#4285F4')
        ),
        secondary_y=True
    )
    
    # Enhanced styling
    fig.update_layout(
        title='Impact of Video Length on Performance',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font={'family': 'Roboto, Arial, sans-serif'},
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=40, b=20),
        hoverlabel=dict(bgcolor='white', font_size=14, font_family='Roboto'),
        height=400
    )
    
    # Update axes titles
    fig.update_xaxes(title_text='Video Length')
    fig.update_yaxes(title_text='Number of Videos', secondary_y=False)
    fig.update_yaxes(title_text='Views per Hour', secondary_y=True)
    
    return fig

def create_hashtags_chart(df):
    """Create an enhanced hashtags chart."""
    if df.empty:
        return empty_chart("No hashtag data available")
    
    # Limit to top 15 for better visualization
    top_df = df.head(15).copy()
    
    # Sort for better visualization
    top_df = top_df.sort_values('count', ascending=True)
    
    fig = px.bar(
        top_df,
        x='count',
        y='hashtag',
        orientation='h',
        color='count',
        color_continuous_scale=px.colors.sequential.Reds,
        labels={
            'count': 'Occurrences',
            'hashtag': 'Hashtag'
        },
        title='Top Trending Hashtags'
    )
    
    # Enhanced styling
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font={'family': 'Roboto, Arial, sans-serif'},
        margin=dict(l=20, r=20, t=40, b=20),
        hoverlabel=dict(bgcolor='white', font_size=14, font_family='Roboto'),
        height=400,
        coloraxis_showscale=False
    )
    
    return fig

def create_time_of_day_chart(df):
    """Create a new chart showing metrics by time of day."""
    if df.empty:
        return empty_chart("No time data available")
    
    # Create subplot with two y-axes
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Bar chart for video count
    fig.add_trace(
        go.Bar(
            x=df['hour_of_day'],
            y=df['video_count'],
            name='Video Count',
            marker_color='#34A853',
            opacity=0.7
        )
    )
    
    # Line chart for views per hour
    fig.add_trace(
        go.Scatter(
            x=df['hour_of_day'],
            y=df['avg_views_per_hour'],
            name='Avg Views/Hour',
            mode='lines+markers',
            marker=dict(size=8, color='#FBBC05'),
            line=dict(width=3, color='#FBBC05')
        ),
        secondary_y=True
    )
    
    # Enhanced styling
    fig.update_layout(
        title='Publishing Time Analysis',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font={'family': 'Roboto, Arial, sans-serif'},
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=40, b=20),
        hoverlabel=dict(bgcolor='white', font_size=14, font_family='Roboto'),
        height=400
    )
    
    # Update axes titles
    fig.update_xaxes(title_text='Hour of Day (24h format)')
    fig.update_yaxes(title_text='Number of Videos', secondary_y=False)
    fig.update_yaxes(title_text='Views per Hour', secondary_y=True)
    
    return fig

def empty_chart(message="No data available"):
    """Create an empty chart with a message."""
    fig = go.Figure()
    fig.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[{
            "text": message,
            "showarrow": False,
            "font": {"size": 20, "color": "#888"}
        }],
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    return fig

# Tab content layouts
category_tab_layout = html.Div([
    html.Div([
        # Categories Chart
        html.Div([
            html.Div([
                html.Div([
                    html.H4("Video Distribution by Category", className="card-title"),
                    dcc.Graph(id="category-chart")
                ], className="card-body")
            ], className="card visualization-card")
        ], className="col-md-7"),
        
        # Top Hashtags
        html.Div([
            html.Div([
                html.Div([
                    html.H4("Top Trending Hashtags", className="card-title"),
                    dcc.Graph(id="hashtags-chart")
                ], className="card-body")
            ], className="card visualization-card")
        ], className="col-md-5"),
    ], className="row mb-4"),
    
    html.Div([
        # Top Channels Table
        html.Div([
            html.Div([
                html.Div([
                    html.H4("Top Channels", className="card-title"),
                    dash_table.DataTable(
                        id='top-channels-table',
                        columns=[
                            {"name": "Channel", "id": "channel_title"},
                            {"name": "Videos", "id": "video_count", "type": "numeric"},
                            {"name": "Avg. Views", "id": "avg_views", "type": "numeric", "format": {"specifier": ",.1f"}},
                            {"name": "Avg. Likes", "id": "avg_likes", "type": "numeric", "format": {"specifier": ",.1f"}},
                            {"name": "Views/Hour", "id": "avg_views_per_hour", "type": "numeric", "format": {"specifier": ",.1f"}}
                        ],
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'padding': '10px',
                            'font-family': 'Roboto, sans-serif'
                        },
                        style_header={
                            'backgroundColor': '#f8f9fa',
                            'fontWeight': 'bold',
                            'borderBottom': '2px solid #dee2e6'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': '#f8f9fa'
                            }
                        ],
                        page_size=5
                    )
                ], className="card-body")
            ], className="card table-card")
        ], className="col-md-12"),
    ], className="row")
])

content_tab_layout = html.Div([
    html.Div([
        # Duration Analysis
        html.Div([
            html.Div([
                html.Div([
                    html.H4("Video Length Impact", className="card-title"),
                    dcc.Graph(id="duration-chart")
                ], className="card-body")
            ], className="card visualization-card")
        ], className="col-md-6"),
        
        # Time of Day Analysis
        html.Div([
            html.Div([
                html.Div([
                    html.H4("Publishing Time Analysis", className="card-title"),
                    dcc.Graph(id="time-of-day-chart")
                ], className="card-body")
            ], className="card visualization-card")
        ], className="col-md-6"),
    ], className="row mb-4"),
    
    html.Div([
        # Engagement Analysis
        html.Div([
            html.Div([
                html.Div([
                    html.H4("Engagement Analysis", className="card-title"),
                    html.Div([
                        html.P("Top performing content by engagement metrics", className="mb-3"),
                        html.Div([
                            html.Div([
                                html.H5("Highest Views/Hour"),
                                html.Div(id="top-views-hour", className="highlight-content")
                            ], className="highlight-card col-md-4"),
                            html.Div([
                                html.H5("Best Like Ratio"),
                                html.Div(id="top-like-ratio", className="highlight-content")
                            ], className="highlight-card col-md-4"),
                            html.Div([
                                html.H5("Most Comments"),
                                html.Div(id="top-comments", className="highlight-content")
                            ], className="highlight-card col-md-4"),
                        ], className="row highlight-container")
                    ])
                ], className="card-body")
            ], className="card analysis-card")
        ], className="col-md-12"),
    ], className="row")
])

top_tab_layout = html.Div([
    html.Div([
        # Top Videos Table
        html.Div([
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
                            className="mb-3 sort-options"
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
                            {"name": "Views/Hour", "id": "views_per_hour", "type": "numeric", "format": {"specifier": ",.1f"}},
                            {"name": "Duration", "id": "duration_display"}
                        ],
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'padding': '10px',
                            'font-family': 'Roboto, sans-serif',
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                            'maxWidth': 0,
                        },
                        style_header={
                            'backgroundColor': '#f8f9fa',
                            'fontWeight': 'bold',
                            'borderBottom': '2px solid #dee2e6'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': '#f8f9fa'
                            }
                        ],
                        page_size=10,
                        tooltip_data=[],
                        tooltip_duration=None
                    )
                ], className="card-body")
            ], className="card table-card")
        ], className="col-md-12"),
    ], className="row mb-4"),
    
    html.Div([
        # Export Options
        html.Div([
            html.Div([
                html.Div([
                    html.H4("Export Data", className="card-title"),
                    html.Div([
                        html.Button("Export to CSV", id="export-csv-btn", className="btn btn-primary me-2"),
                        html.Button("Export to Excel", id="export-excel-btn", className="btn btn-success me-2"),
                        html.Button("Export Charts", id="export-charts-btn", className="btn btn-info"),
                        html.Div(id="export-status", className="mt-3")
                    ], className="export-options")
                ], className="card-body")
            ], className="card export-card")
        ], className="col-md-12"),
    ], className="row")
])

# Define app layout
app.layout = html.Div([
    # Header
    html.Div([
        html.Div([
            html.Img(src='/assets/youtube-logo.png', className="youtube-logo"),
            html.H1("YouTube Trending Analysis", className="header-title"),
            html.P("Interactive dashboard for YouTube trending videos analytics", className="header-subtitle"),
        ], className="header-content col-md-8"),
        
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
            html.Div(id='batch-info', className="batch-info")
        ], className="col-md-4"),
    ], className="dashboard-header"),
    
    # Filters
    html.Div([
        html.Div([
            html.Label("Category Filter:"),
            dcc.Dropdown(
                id='category-filter',
                options=[],
                multi=True,
                placeholder="All Categories",
                className="form-select"
            ),
        ], className="col-md-4"),
        
        html.Div([
            html.Label("Minimum Views:"),
            dcc.Slider(
                id='views-slider',
                min=0,
                max=1000000,
                step=50000,
                marks={i: f'{i/1000:.0f}K' for i in range(0, 1000001, 250000)},
                value=0,
                className="views-slider"
            ),
        ], className="col-md-4"),
        
        html.Div([
            html.Label("Time Range:"),
            dcc.DatePickerRange(
                id='date-range',
                start_date_placeholder_text="Start Date",
                end_date_placeholder_text="End Date",
                className="date-picker"
            ),
        ], className="col-md-4"),
    ], className="filters-section row"),
    
    # Stats Cards
    html.Div([
        html.Div([
            html.Div([
                html.Div([
                    html.I(className="fas fa-video stats-icon"),
                    html.Div([
                        html.H5("Total Videos", className="card-title"),
                        html.H2(id="total-videos", className="stats-value"),
                    ], className="stats-text")
                ], className="stats-content")
            ], className="stats-card")
        ], className="col-md-3"),
        
        html.Div([
            html.Div([
                html.Div([
                    html.I(className="fas fa-eye stats-icon"),
                    html.Div([
                        html.H5("Avg. Views", className="card-title"),
                        html.H2(id="avg-views", className="stats-value"),
                        html.Span(id="views-trend", className="trend-indicator")
                    ], className="stats-text")
                ], className="stats-content")
            ], className="stats-card")
        ], className="col-md-3"),
        
        html.Div([
            html.Div([
                html.Div([
                    html.I(className="fas fa-thumbs-up stats-icon"),
                    html.Div([
                        html.H5("Avg. Likes", className="card-title"),
                        html.H2(id="avg-likes", className="stats-value"),
                        html.Span(id="likes-trend", className="trend-indicator")
                    ], className="stats-text")
                ], className="stats-content")
            ], className="stats-card")
        ], className="col-md-3"),
        
        html.Div([
            html.Div([
                html.Div([
                    html.I(className="fas fa-comment stats-icon"),
                    html.Div([
                        html.H5("Avg. Views/Hour", className="card-title"),
                        html.H2(id="avg-vph", className="stats-value"),
                        html.Span(id="vph-trend", className="trend-indicator")
                    ], className="stats-text")
                ], className="stats-content")
            ], className="stats-card")
        ], className="col-md-3"),
    ], className="stats-section row"),
    
    # Insights Section
    html.Div([
        html.Div([
            html.Div([
                html.H4("Key Insights", className="card-title"),
                html.Div(id="insights-content", className="insights-container")
            ], className="card-body")
        ], className="card insights-card"),
    ], className="insights-section row"),
    
    # Tabs for different analyses
    html.Div([
        dcc.Tabs(id="analysis-tabs", value="category-tab", className="custom-tabs", children=[
            dcc.Tab(label="Category Analysis", value="category-tab", className="custom-tab", 
                    selected_className="custom-tab--selected"),
            dcc.Tab(label="Content Analysis", value="content-tab", className="custom-tab", 
                    selected_className="custom-tab--selected"),
            dcc.Tab(label="Top Performers", value="top-tab", className="custom-tab", 
                    selected_className="custom-tab--selected"),
        ]),
        html.Div(id="tab-content", className="tab-content")
    ], className="tabs-section"),
    
    # Footer
    html.Footer([
        html.Div([
            html.P("YouTube Trending Analysis Dashboard | Data Engineering Portfolio", className="footer-text"),
            html.P("Built with Dash, Plotly, and Python", className="footer-text small")
        ], className="footer-content")
    ], className="dashboard-footer"),
    
    # Load FontAwesome for icons
    html.Link(
        rel="stylesheet",
        href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css"
    )
], className="dashboard-container")

# Callbacks
@app.callback(
    Output('batch-dropdown', 'options'),
    Output('batch-dropdown', 'value'),
    Input('batch-dropdown', 'options')  # This is just a dummy input to trigger on load
)
@cache.memoize(expire=300)  # Cache for 5 minutes - Fixed parameter name
def update_batch_dropdown(existing_options):
    """Update the batch dropdown options and select the latest batch."""
    batches = get_available_batches()
    options = [{'label': f"Batch {batch}", 'value': batch} for batch in batches]
    return options, batches[0] if batches else None

@app.callback(
    Output('category-filter', 'options'),
    Input('batch-dropdown', 'value')
)
@cache.memoize(expire=300)  # Cache for 5 minutes - Fixed parameter name
def update_category_filter(batch_id):
    """Update the category filter options based on the selected batch."""
    if not batch_id:
        return []
    
    categories = get_available_categories()
    options = [{'label': cat_name, 'value': cat_id} for cat_id, cat_name in categories.items()]
    return options

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
        try:
            dt = pd.to_datetime(timestamp)
            formatted_time = dt.strftime("%B %d, %Y at %H:%M")
            return f"Data extracted on: {formatted_time}"
        except:
            return f"Data extracted on: {timestamp}"
    return "Timestamp not available"

@app.callback(
    Output('tab-content', 'children'),
    Input('analysis-tabs', 'value')
)
def render_tab_content(tab):
    """Render the content of the selected tab."""
    if tab == 'category-tab':
        return category_tab_layout
    elif tab == 'content-tab':
        return content_tab_layout
    elif tab == 'top-tab':
        return top_tab_layout
    return html.Div("Tab content not found.")

@app.callback(
    [
        Output('total-videos', 'children'),
        Output('avg-views', 'children'),
        Output('avg-likes', 'children'),
        Output('avg-vph', 'children'),
        Output('views-trend', 'children'),
        Output('likes-trend', 'children'),
        Output('vph-trend', 'children'),
        Output('views-trend', 'className'),
        Output('likes-trend', 'className'),
        Output('vph-trend', 'className'),
    ],
    [
        Input('batch-dropdown', 'value'),
        Input('category-filter', 'value')
    ]
)
@cache.memoize(expire=300)  # Cache for 5 minutes - Fixed parameter name
def update_summary_stats(batch_id, category_ids):
    """Update the summary statistics cards."""
    if not batch_id:
        empty_vals = ["0"] * 4 + [""] * 3 + ["trend-indicator"] * 3
        return empty_vals
    
    summary = get_dashboard_summary(batch_id, category_ids)
    growth = get_trending_growth(batch_id, category_ids)
    
    # Format summary values - FIXED VERSION
    if summary is not None:
        try:
            total_videos = f"{int(summary['total_videos']):,}" 
        except (ValueError, TypeError, KeyError):
            total_videos = "0"
            
        try:
            avg_views = f"{int(summary['avg_views']):,}" 
        except (ValueError, TypeError, KeyError):
            avg_views = "0"
            
        try:
            avg_likes = f"{int(summary['avg_likes']):,}" 
        except (ValueError, TypeError, KeyError):
            avg_likes = "0"
            
        try:
            avg_vph = f"{int(summary['avg_views_per_hour']):,}" 
        except (ValueError, TypeError, KeyError):
            avg_vph = "0"
    else:
        total_videos = avg_views = avg_likes = avg_vph = "0"
    
    # Trend indicators
    if growth:
        try:
            views_trend = f"{growth['views_growth']:.1f}%" if abs(growth['views_growth']) > 0.1 else ""
        except (KeyError, TypeError):
            views_trend = ""
            
        try:    
            likes_trend = f"{growth['likes_growth']:.1f}%" if abs(growth['likes_growth']) > 0.1 else ""
        except (KeyError, TypeError):
            likes_trend = ""
            
        try:
            vph_trend = f"{growth['views_per_hour_growth']:.1f}%" if abs(growth['views_per_hour_growth']) > 0.1 else ""
        except (KeyError, TypeError):
            vph_trend = ""
            
        try:
            views_class = "trend-indicator " + ("trend-up" if growth['views_growth'] >= 0 else "trend-down")
        except (KeyError, TypeError):
            views_class = "trend-indicator"
            
        try:
            likes_class = "trend-indicator " + ("trend-up" if growth['likes_growth'] >= 0 else "trend-down")
        except (KeyError, TypeError):
            likes_class = "trend-indicator"
            
        try:
            vph_class = "trend-indicator " + ("trend-up" if growth['views_per_hour_growth'] >= 0 else "trend-down")
        except (KeyError, TypeError):
            vph_class = "trend-indicator"
    else:
        views_trend = likes_trend = vph_trend = ""
        views_class = likes_class = vph_class = "trend-indicator"
    
    return total_videos, avg_views, avg_likes, avg_vph, views_trend, likes_trend, vph_trend, views_class, likes_class, vph_class

@app.callback(
    Output('insights-content', 'children'),
    [
        Input('batch-dropdown', 'value'),
        Input('category-filter', 'value')
    ]
)
@cache.memoize(expire=300)  # Cache for 5 minutes - Fixed parameter name
def update_insights(batch_id, category_ids):
    """Update the automated insights section."""
    if not batch_id:
        return html.Div("No insights available. Select a batch to see insights.")
    
    insights = generate_insights(batch_id, category_ids)
    
    if not insights:
        return html.Div("No insights available for the selected data.")
    
    insight_cards = []
    
    for insight in insights:
        card = html.Div([
            html.Div([
                html.I(className=f"fas fa-{insight['icon']} insight-icon"),
                html.Div([
                    html.H5(insight['title'], className="insight-title"),
                    html.Div([
                        html.Span(insight['value'], className="insight-value"),
                        html.Span(insight['metric'], className="insight-metric"),
                    ], className="insight-data")
                ], className="insight-content")
            ], className="insight-inner")
        ], className="insight-card col-md-3")
        
        insight_cards.append(card)
    
    return html.Div(insight_cards, className="row")

@app.callback(
    [
        Output('category-chart', 'figure'),
        Output('hashtags-chart', 'figure'),
        Output('top-channels-table', 'data'),
    ],
    [
        Input('batch-dropdown', 'value'),
        Input('category-filter', 'value')
    ]
)
@cache.memoize(expire=300)  # Cache for 5 minutes - Fixed parameter name
def update_category_tab(batch_id, category_ids):
    """Update the category analysis tab content."""
    if not batch_id:
        empty_fig = empty_chart("No data available")
        return empty_fig, empty_fig, []
    
    # Get data
    category_data = get_category_data(batch_id, category_ids)
    hashtag_data = get_top_hashtags(batch_id, category_ids=category_ids)
    top_channels = get_top_channels(batch_id, category_ids=category_ids)
    
    # Create visualizations
    category_fig = create_category_chart(category_data)
    hashtags_fig = create_hashtags_chart(hashtag_data)
    
    return category_fig, hashtags_fig, top_channels.to_dict('records')

@app.callback(
    [
        Output('duration-chart', 'figure'),
        Output('time-of-day-chart', 'figure'),
        Output('top-views-hour', 'children'),
        Output('top-like-ratio', 'children'),
        Output('top-comments', 'children'),
    ],
    [
        Input('batch-dropdown', 'value'),
        Input('category-filter', 'value')
    ]
)
@cache.memoize(expire=300)  # Cache for 5 minutes - Fixed parameter name
def update_content_tab(batch_id, category_ids):
    """Update the content analysis tab content."""
    if not batch_id:
        empty_fig = empty_chart("No data available")
        empty_highlight = html.Div("No data available", className="highlight-empty")
        return empty_fig, empty_fig, empty_highlight, empty_highlight, empty_highlight
    
    # Get data
    duration_data = get_duration_stats(batch_id, category_ids)
    time_data = get_time_of_day_stats(batch_id, category_ids)
    top_videos = get_top_videos(batch_id, category_ids=category_ids)
    
    # Create visualizations
    duration_fig = create_duration_chart(duration_data)
    time_fig = create_time_of_day_chart(time_data)
    
    # Create highlight cards
    if not top_videos.empty:
        # Top views per hour
        top_vph_video = top_videos.sort_values('views_per_hour', ascending=False).iloc[0]
        top_vph_content = html.Div([
            html.P(top_vph_video['title'], className="highlight-title"),
            html.P([
                html.Span(f"{int(top_vph_video['views_per_hour']):,}"),
                html.Span(" views/hour")
            ], className="highlight-value"),
            html.P(f"Channel: {top_vph_video['channel_title']}", className="highlight-detail")
        ])
        
        # Top like ratio
        top_like_video = top_videos.sort_values('like_view_ratio', ascending=False).iloc[0]
        top_like_content = html.Div([
            html.P(top_like_video['title'], className="highlight-title"),
            html.P([
                html.Span(f"{top_like_video['like_view_ratio']:.2f}%"),
                html.Span(" like ratio")
            ], className="highlight-value"),
            html.P(f"Channel: {top_like_video['channel_title']}", className="highlight-detail")
        ])
        
        # Top comments
        top_comment_video = top_videos.sort_values('comment_count', ascending=False).iloc[0]
        top_comment_content = html.Div([
            html.P(top_comment_video['title'], className="highlight-title"),
            html.P([
                html.Span(f"{int(top_comment_video['comment_count']):,}"),
                html.Span(" comments")
            ], className="highlight-value"),
            html.P(f"Channel: {top_comment_video['channel_title']}", className="highlight-detail")
        ])
    else:
        empty_highlight = html.Div("No data available", className="highlight-empty")
        top_vph_content = top_like_content = top_comment_content = empty_highlight
    
    return duration_fig, time_fig, top_vph_content, top_like_content, top_comment_content

@app.callback(
    [
        Output('top-videos-table', 'data'),
        Output('top-videos-table', 'tooltip_data')
    ],
    [
        Input('batch-dropdown', 'value'),
        Input('video-sort-options', 'value'),
        Input('category-filter', 'value'),
        Input('views-slider', 'value')
    ]
)
@cache.memoize(expire=300)  # Cache for 5 minutes - Fixed parameter name
def update_top_videos_table(batch_id, sort_by, category_ids, min_views):
    """Update the top videos table."""
    if not batch_id:
        return [], []
    
    # Get top videos
    top_videos = get_top_videos(batch_id, sort_by=sort_by, category_ids=category_ids, min_views=min_views)
    
    if top_videos.empty:
        return [], []
    
    # Add duration in human readable format
    def format_duration(seconds):
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    top_videos['duration_display'] = top_videos['duration_seconds'].apply(format_duration)
    
    # Create tooltip data for truncated cells
    tooltip_data = []
    for i in range(len(top_videos)):
        tooltip_data.append({
            'title': {'value': top_videos.iloc[i]['title'], 'type': 'markdown'},
            'channel_title': {'value': top_videos.iloc[i]['channel_title'], 'type': 'markdown'}
        })
    
    return top_videos.to_dict('records'), tooltip_data

@app.callback(
    Output('export-status', 'children'),
    [
        Input('export-csv-btn', 'n_clicks'),
        Input('export-excel-btn', 'n_clicks'),
        Input('export-charts-btn', 'n_clicks')
    ],
    [
        State('batch-dropdown', 'value'),
        State('category-filter', 'value')
    ],
    prevent_initial_call=True
)
def handle_export(csv_clicks, excel_clicks, charts_clicks, batch_id, category_ids):
    """Handle export button clicks."""
    ctx = dash.callback_context
    
    if not ctx.triggered:
        raise PreventUpdate
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if not batch_id:
        return html.Div("No data available to export", className="export-message error")
    
    # This would typically trigger a download, but in Dash we need to use dcc.Download
    # For simplicity, we'll just show a message
    if button_id == 'export-csv-btn':
        return html.Div([
            html.I(className="fas fa-check-circle me-2"),
            "Data would be exported as CSV. In a production environment, this would trigger a download."
        ], className="export-message success")
    
    elif button_id == 'export-excel-btn':
        return html.Div([
            html.I(className="fas fa-check-circle me-2"),
            "Data would be exported as Excel. In a production environment, this would trigger a download."
        ], className="export-message success")
        
    elif button_id == 'export-charts-btn':
        return html.Div([
            html.I(className="fas fa-check-circle me-2"),
            "Charts would be exported as images. In a production environment, this would trigger a download."
        ], className="export-message success")
    
    return html.Div("Unknown export option", className="export-message error")

if __name__ == "__main__":
    app.run_server(
        host=config['dashboard']['host'],
        port=config['dashboard']['port'],
        debug=config['dashboard']['debug']
    )
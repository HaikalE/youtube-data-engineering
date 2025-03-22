#!/usr/bin/env python3
"""
Script to insert sample data into the YouTube trending database.
This can be run independently if the pipeline is not loading data correctly.
"""

import sqlite3
import datetime
import json
import random
import os

# Connect to the database
db_path = 'youtube_trending.db'
print(f"Connecting to database: {db_path}")

if not os.path.exists(db_path):
    print(f"Error: Database file {db_path} does not exist.")
    print("Please make sure the database has been initialized by running the start.sh script.")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Generate a batch ID
batch_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
print(f"Using batch ID: {batch_id}")

# Create sample data
video_count = 100
print(f"Preparing to insert {video_count} sample trending videos...")

# Create sample categories
categories = [
    (1, "Film & Animation"),
    (2, "Autos & Vehicles"),
    (10, "Music"),
    (15, "Pets & Animals"),
    (17, "Sports"),
    (20, "Gaming"),
    (22, "People & Blogs"),
    (23, "Comedy"),
    (24, "Entertainment"),
    (25, "News & Politics"),
    (26, "Howto & Style"),
    (28, "Science & Technology")
]

# Create sample channels
channels = [
    (f"channel_{i}", f"Sample Channel {i}") for i in range(10)
]

# Sample length categories
length_categories = ["< 1 min", "1-5 min", "5-10 min", "10-20 min", "> 20 min"]

try:
    # Insert videos
    for i in range(video_count):
        # Pick a random category and channel
        cat_id, cat_name = random.choice(categories)
        channel_id, channel_name = random.choice(channels)
        
        # Generate publish time (between 1 day and 30 days ago)
        hours_ago = random.randint(24, 24*30)
        publish_time = (datetime.datetime.now() - datetime.timedelta(hours=hours_ago)).isoformat()
        extracted_at = datetime.datetime.now().isoformat()
        
        # Generate random metrics
        view_count = random.randint(10000, 1000000)
        like_count = int(view_count * random.uniform(0.01, 0.2))
        comment_count = int(view_count * random.uniform(0.001, 0.05))
        
        # Calculate derived metrics
        views_per_hour = view_count / hours_ago
        like_view_ratio = (like_count / view_count) * 100
        comment_view_ratio = (comment_count / view_count) * 100
        
        # Random duration
        duration_seconds = random.randint(60, 3600)
        
        # Determine length category
        if duration_seconds < 60:
            length_category = "< 1 min"
        elif duration_seconds < 300:
            length_category = "1-5 min"
        elif duration_seconds < 600:
            length_category = "5-10 min"
        elif duration_seconds < 1200:
            length_category = "10-20 min"
        else:
            length_category = "> 20 min"
        
        # Insert the video
        cursor.execute('''
        INSERT INTO trending_videos (
            batch_id, video_id, title, channel_id, channel_title, 
            category_id, category_name, view_count, like_count, comment_count,
            publish_time, extracted_at, duration_seconds, length_category,
            hours_since_published, views_per_hour, like_view_ratio, comment_view_ratio,
            all_hashtags, tags
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            batch_id, 
            f'sample_{batch_id}_{i}', 
            f'Sample Video {i} - {cat_name}',
            channel_id,
            channel_name,
            cat_id,
            cat_name,
            view_count,
            like_count,
            comment_count,
            publish_time,
            extracted_at,
            duration_seconds,
            length_category,
            hours_ago,
            views_per_hour,
            like_view_ratio,
            comment_view_ratio,
            json.dumps([f"tag{i % 5}", f"popular{i % 3}"]),  # sample hashtags
            json.dumps([f"tag{j}" for j in range(3)])  # sample tags
        ))
    
    # Calculate channel stats
    print("Calculating channel statistics...")
    cursor.execute(f'''
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
    ''')

    # Calculate trends summary
    print("Calculating trends summary...")
    cursor.execute(f'''
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
    ''')

    # Insert hashtags (simplified)
    print("Calculating hashtag statistics...")
    for i in range(20):
        cursor.execute('''
        INSERT INTO hashtags (
            batch_id, hashtag, count, category_id, category_name, extracted_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            batch_id,
            f"hashtag_{i}",
            random.randint(5, 50),
            random.choice(categories)[0],
            random.choice(categories)[1],
            datetime.datetime.now().isoformat()
        ))

    # Commit all changes
    conn.commit()
    print(f"✅ Successfully inserted {video_count} sample records with batch_id {batch_id}")

except Exception as e:
    conn.rollback()
    print(f"❌ Error inserting sample data: {str(e)}")
    import traceback
    traceback.print_exc()
finally:
    conn.close()

print("\nNext steps:")
print("1. Run './start.sh dashboard' to start the dashboard")
print("2. Access the dashboard at http://localhost:8050")
print("3. Select the batch ID from the dropdown to see the data")
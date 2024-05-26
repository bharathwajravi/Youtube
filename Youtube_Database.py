import mysql.connector
from googleapiclient.discovery import build
import pandas as pd
import html
import re


#API Key
api_key = 'AIzaSyC8k1Xg6fzVS-T_rtjl9kRFAoAjV5VlgaI'


# Connecting to MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="charan"
)
mycursor = mydb.cursor()

# Creating a database called Youtube
db_query1 = "CREATE DATABASE IF NOT EXISTS Youtube"
mycursor.execute(db_query1)
mydb.database = "Youtube"

# Creating Channel table in Youtube Database
channel_table = """
CREATE TABLE IF NOT EXISTS Channel(
channel_id VARCHAR(255) PRIMARY KEY,
channel_name VARCHAR(255),
channel_type VARCHAR(255),
channel_views BIGINT,
channel_description TEXT,
channel_status VARCHAR(255)
)
"""

# Creating comment table in Youtube Database
comment_table = """
CREATE TABLE IF NOT EXISTS Comment(
comment_id VARCHAR(255) PRIMARY KEY,
video_id VARCHAR(255),
comment_text TEXT,
comment_author VARCHAR(255),
comment_published_date DATETIME,
FOREIGN KEY(video_id) REFERENCES Video(video_id)
)
"""

# Creating video table in Youtube Database
video_table = """
CREATE TABLE IF NOT EXISTS Video(
    video_id VARCHAR(255) PRIMARY KEY,
    channel_id VARCHAR(255),
    video_name VARCHAR(255),
    video_description TEXT,
    published_date DATETIME,
    view_count BIGINT,
    like_count BIGINT,
    favorite_count BIGINT,
    comment_count BIGINT,
    duration INT,
    thumbnail VARCHAR(255),
    caption_status VARCHAR(255),
    FOREIGN KEY (channel_id) REFERENCES Channel(channel_id)
)
"""

# First, create Channel table
mycursor.execute(channel_table)

# Then, create Video table
mycursor.execute(video_table)

# Finally, create Comment table
mycursor.execute(comment_table)

# This is a function to collect channel information
def get_channel_details(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.channels().list(
        part='snippet, statistics',
        id=channel_id
    )
    response = request.execute()

    if 'items' in response:
        channel_info = response['items'][0]
        channel_data = {
            'channel_id': channel_info['id'],
            'channel_name': channel_info['snippet']['title'],
            'channel_description': channel_info['snippet']['description'],
            'channel_views': channel_info['statistics']['viewCount'],
            'channel_type': channel_info['snippet'].get('channel_type', 'Unknown'),
            'channel_status': channel_info['snippet'].get('liveBroadcastContent', 'Unknown')
        }
        return channel_data
    else:
        print('Channel not found')
        return None



# This function is to get video details
def get_video_details(api_key, video_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.videos().list(
        part='snippet, statistics, contentDetails',
        id=video_id
    )
    response = request.execute()

    if 'items' in response:
        video_info = response['items'][0]
        channel_id = video_info['snippet']['channelId']  # Extracting channel ID
        video_data = {
            'video_id': video_info['id'],
            'channel_id': channel_id,
            'video_name': video_info['snippet']['title'],
            'video_description': video_info['snippet']['description'],
            'published_date': video_info['snippet']['publishedAt'],
            'view_count': video_info['statistics']['viewCount'],
            'like_count': video_info['statistics']['likeCount'],
            'favorite_count': video_info['statistics']['favoriteCount'],
            'comment_count': video_info['statistics']['commentCount'],
            'duration': video_info.get('contentDetails', {}).get('duration', 'Unknown'),
            'thumbnail': video_info['snippet']['thumbnails']['default']['url'],
            'caption_status': video_info.get('contentDetails', {}).get('caption', 'Unknown')
        }
        return video_data
    else:
        print('Video not found')
    return None



# This function is to get video ID from URL
def get_video_id_from_url(url):
    if 'youtu.be/' in url:
        start_index = url.index('youtu.be/') + len('youtu.be/')
        end_index = url.find('?', start_index)
        if end_index == -1:
            end_index = len(url)
        return url[start_index:end_index]
    elif 'v=' in url:
        start_index = url.index('v=') + 2
        end_index = url.find('&', start_index)
        if end_index == -1:
            end_index = len(url)
        return url[start_index:end_index]
    else:
        print("Invalid YouTube URL")
        return None

# This function is to get the comments of the video
def get_video_comments(api_key, video_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id
    )
    response = request.execute()

    comments_data = []

    if 'items' in response:
        for comment in response['items']:
            comment_data = {
                'comment_id': comment['id'],
                'video_id': video_id,
                'comment_text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                'comment_author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_published_date': comment['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments_data.append(comment_data)

    return comments_data

# This function is to clean and store data
def clean_and_store_data( channel_details, video_details, comments):
    # Clean channel details
    channel_details_cleaned = {
    'channel_id': channel_details['channel_id'],
    'channel_name': channel_details['channel_name'],
    'channel_description': channel_details['channel_description'],
    'channel_views': int(channel_details['channel_views']),
    'channel_type': channel_details['channel_type'],  
    'channel_status': channel_details['channel_status']
 }

    # Clean video details
    video_details_cleaned = {
        'video_id': video_details['video_id'],
        'channel_id': channel_id,
        'video_name': video_details['video_name'],
        'video_description': video_details['video_description'],
        'published_date': pd.to_datetime(video_details['published_date']),
        'view_count': int(video_details['view_count']),
        'like_count': int(video_details['like_count']),
        'favorite_count': int(video_details['favorite_count']),
        'comment_count': int(video_details['comment_count']),
        'duration': video_details['duration'],
        'thumbnail': video_details['thumbnail'],
        'caption_status': video_details['caption_status']
    }

    # Clean comments
    comments_cleaned = []
    for comment in comments:
        comment_cleaned = {
            'comment_id': comment['comment_id'],
            'video_id': comment['video_id'],
            'comment_text': html.unescape(comment['comment_text']),
            'comment_author': comment['comment_author'],
            'comment_published_date': pd.to_datetime(comment['comment_published_date'])
        }
        comments_cleaned.append(comment_cleaned)

    # Convert to DataFrames
    channel_df = pd.DataFrame([channel_details_cleaned])
    video_df = pd.DataFrame([video_details_cleaned])
    comments_df = pd.DataFrame(comments_cleaned)

    return channel_df, video_df, comments_df

def insert_channel_data_to_db(cursor, channel_df):
    # Convert DataFrame rows to tuples
    channel_data_tuples = [tuple(row) for row in channel_df.itertuples(index=False, name=None)]
    
    # Prepare the SQL Insert Statement
    insert_channel_query = """
    INSERT INTO Channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    #  Execute the Insert Statement
    for data_tuple in channel_data_tuples:
        data_tuple = list(data_tuple)
        data_tuple[2] = data_tuple[2][:255]
        cursor.execute(insert_channel_query, tuple(data_tuple))


def convert_duration_to_seconds(duration):
    # Regular expression to parse the duration string
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    matches = pattern.match(duration)
    if matches:
        hours = int(matches.group(1)) if matches.group(1) else 0
        minutes = int(matches.group(2)) if matches.group(2) else 0
        seconds = int(matches.group(3)) if matches.group(3) else 0
        return hours * 3600 + minutes * 60 + seconds
    else:
        return 0

def insert_video_data_to_db(cursor, video_df):
    # Convert DataFrame rows to tuples
    video_data_tuples = [tuple(row) for row in video_df.itertuples(index=False, name=None)]
    
    # Prepare the SQL Insert Statement
    insert_video_query = """
    INSERT INTO Video (video_id, channel_id, video_name, video_description, published_date, view_count, 
                       like_count, favorite_count, comment_count, duration, thumbnail, caption_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Execute the Insert Statement
    for data_tuple in video_data_tuples:
        try:
            # Convert duration to seconds
            duration_idx = 9  # Index of the duration column in the tuple
            data_tuple = list(data_tuple)
            data_tuple[duration_idx] = convert_duration_to_seconds(data_tuple[duration_idx])
            
            # Execute the query
            cursor.execute(insert_video_query, tuple(data_tuple))
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            print(f"Failed to insert data: {data_tuple}")
    
    print("Video data inserted.")



def insert_comment_data_to_db(cursor, comments_df):
    # Convert DataFrame rows to tuples
    comments_data_tuples = [tuple(row) for row in comments_df.itertuples(index=False, name=None)]
    
    # Prepare the SQL Insert Statement
    insert_comment_query = """
    INSERT INTO Comment (comment_id, video_id, comment_text, comment_author, comment_published_date)
    VALUES (%s, %s, %s, %s, %s)
    """
    
    # Execute the Insert Statement
    for data_tuple in comments_data_tuples:
        try:
            cursor.execute(insert_comment_query, tuple(data_tuple))
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            print(f"Failed to insert data: {data_tuple}")
    
    print("Comment data inserted.")



# Enter Channel ID
channel_id = input('Enter the channel ID: ')

# Get channel details
channel_details = get_channel_details(api_key, channel_id)

# Assuming you have a video URL to work with
video_url = input('Enter the video URL: ')
video_id = get_video_id_from_url(video_url)

# Get video details
video_details = get_video_details(api_key, video_id)

# Get video comments
comments = get_video_comments(api_key, video_id)

# Clean and store data
channel_df, video_df, comments_df = clean_and_store_data(channel_details, video_details, comments)


# Inserting channel data in the channel table of Youtube database
insert_channel_data_to_db(mycursor, channel_df)
mydb.commit()

# Execute the SELECT query
mycursor.execute("SELECT * FROM Channel")

# Fetch all rows
result = mycursor.fetchall()

# Print the result
for row in result:
    print(row)

# Inserting video data in the video table of Youtube database
insert_video_data_to_db(mycursor, video_df)
mydb.commit()

# Execute the SELECT query for Video
mycursor.execute("SELECT * FROM Video")
result_video = mycursor.fetchall()

# Print the result from Video table
print("Video Table:")
for row in result_video:
    print(row)

# Inserting comment data into the Comment table of the YouTube database
insert_comment_data_to_db(mycursor, comments_df)
mydb.commit()

# Execute the SELECT query for Comment
mycursor.execute("SELECT * FROM Comment")
result_comment = mycursor.fetchall()

# Print the result from Comment table
print("Comment Table:")
for row in result_comment:
    print(row)


# Print comment IDs
print("Comment IDs:")
for comment in comments:
    print(comment['comment_id'])





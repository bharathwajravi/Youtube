import mysql.connector
import streamlit as st
import pandas as pd

# Connecting to MySQL
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="charan",
        database="Youtube"
    )

# Function to search the database based on user input
def search_database(query):
    mydb = create_connection()
    mycursor = mydb.cursor()
    mycursor.execute(query)
    result = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    return result

# Function to extract Video ID from URL
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
        st.error("Invalid YouTube URL")
        return None

# Streamlit UI
def main():
    st.sidebar.title('Menu')
    option = st.sidebar.selectbox('Choose an option:', ['Details', 'Questions'])

    if option == 'Details':
        st.title('YouTube Database Search')

        search_option = st.radio("Search by:", ("Channel", "Video", "Comment"))
        
        # To search channel details
        if search_option == "Channel":
            channel_id = st.text_input("Enter Channel ID:")
            if st.button("Search"):
                if channel_id:
                    query = f"SELECT * FROM Channel WHERE channel_id='{channel_id}'"
                    result = search_database(query)
                    if result:
                        st.write("Channel Details:")
                        for channel_info in result:
                            st.write(f"Channel ID: {channel_info[0]}")
                            st.write(f"Channel Name: {channel_info[1]}")
                            st.write("Description:")
                            st.write(channel_info[2])
                            st.write(f"Subscriber Count: {channel_info[3]}")
                            st.write(f"Country: {channel_info[4]}")
                            st.write(f"Category: {channel_info[5]}")
                    else:
                        st.write("No channel found with the provided ID.")
        # To search video details
        elif search_option == "Video":
            video_url = st.text_input("Enter Video URL:")
            if st.button("Search"):
                video_id = get_video_id_from_url(video_url)
                if video_id:
                    query = f"SELECT * FROM Video WHERE video_id='{video_id}'"
                    result = search_database(query)
                    if result:
                        st.write("Video Details:")
                        st.write(result)
                    else:
                        st.write("No video found with the provided URL.")
                        
        # To search comment details
        elif search_option == "Comment":
            comment_id = st.text_input("Enter Comment ID:")
            if st.button("Search"):
                if comment_id:
                    query = f"SELECT * FROM Comment WHERE comment_id='{comment_id}'"
                    result = search_database(query)
                    if result:
                        st.write("Comment Details:")
                        st.write(result)
                    else:
                        st.write("No comment found with the provided ID.")

    elif option == 'Questions':
     st.title('Questions')
     with st.expander("What are the names of all the videos and their corresponding channels?"):
      
        result = []

      
        query = """
        SELECT Video.video_name, Channel.channel_name 
        FROM Video 
        JOIN Channel ON Video.channel_id = Channel.channel_id
        """
        result = search_database(query)
        
        # Displaying the results in a table
        if result:
            st.write("Names of Videos along with their channels: ")
            df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
            st.write(df)
        else:
            st.write("No data found.")
    
     with st.expander("Which channels have the most number of videos, and how many videos do they have?"):
       
        result = []

        
        query = """
        SELECT Channel.channel_name, COUNT(Video.video_id) AS video_count
        FROM Video
        JOIN Channel ON Video.channel_id = Channel.channel_id
        GROUP BY Channel.channel_name
        ORDER BY video_count DESC
        """
        result = search_database(query)
        
        # Displaying the results in a table
        if result:
            st.write("Channles with most number of videos: ")
            df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
            st.write(df)
        else:
            st.write("No data found.")

     with st.expander("How many comments were made on each video, and what are there corresponding video names?"):
       
        result = []

        
        query = """
       SELECT Video.video_name, COUNT(Comment.comment_id) AS comment_count
            FROM Video
            LEFT JOIN Comment ON Video.video_id = Comment.video_id
            GROUP BY Video.video_name
        """
        result = search_database(query)
        
        # Displaying the results in a table
        if result:
            st.write("Number of comments made on each video along with their video names: ")
            df = pd.DataFrame(result, columns=["Video Name", "Comment Count"])
            st.write(df)
        else:   
            st.write("No data found.")
            
     with st.expander("Which videos have the highest number of likes, and what are their corresponding channel names?"):
       
        result = []

        
        query = """
        SELECT Video.video_name, Channel.channel_name, Video.like_count
        FROM Video
        JOIN Channel ON Video.channel_id = Channel.channel_id
        ORDER BY Video.like_count DESC
        LIMIT 10;

        """
        result = search_database(query)
        
        # Displaying the results in a table
        if result:
            st.write("Top 10 highest number of likes and their corresponding channel names: ")
            df = pd.DataFrame(result, columns=["Video Name", "Channel Name","Like Count"])
            st.write(df)
        else:   
            st.write("No data found.")
     
     with st.expander("What is the total number of views for each channel, and what are their corresponding channel names?"):
       
        result = []

        
        query = """
        SELECT Channel.channel_name, SUM(Video.view_count) AS total_views
        FROM Video
        JOIN Channel ON Video.channel_id = Channel.channel_id
        GROUP BY Channel.channel_name;
        """
        result = search_database(query)
        
        # Displaying the results in a table
        if result:
            st.write("Total number of views in each channel and their corresponding channel names: ")
            df = pd.DataFrame(result, columns=["Channel Name", "Total Views"])
            st.write(df)
        else:   
            st.write("No data found.")
     
     with st.expander("What are the names of all the channels that have published videos in the year 2022?"):
       
        result = []

        
        query = """
        SELECT DISTINCT Channel.channel_name
        FROM Video
        JOIN Channel ON Video.channel_id = Channel.channel_id
        WHERE YEAR(Video.published_date) = 2022;

        """
        result = search_database(query)
        
        # Displaying the results in a table
        if result:
            st.write("Channels that have published videos in the year 2022: ")
            df = pd.DataFrame(result, columns=["Channel Name"])
            st.write(df)
        else:   
            st.write("No data found.")
     
     with st.expander("What is the average duration of all videos in each channel, and what are their corresponding channel names?"):
       
        result = []

        
        query = """
        SELECT Channel.channel_name, AVG(Video.duration) AS average_duration
        FROM Video
        JOIN Channel ON Video.channel_id = Channel.channel_id
        GROUP BY Channel.channel_name;


        """
        result = search_database(query)
        
        # Displaying the results in a table
        if result:
            st.write("Average duration of all videos in each channel: ")
            df = pd.DataFrame(result, columns=["Channel Name","Average Duration"])
            st.write(df)
        else:   
            st.write("No data found.")
     
     with st.expander("Which videos have the highest number of comments, and what are their corresponding channel names?"):
       
        result = []

        
        query = """
        SELECT Video.video_name, Channel.channel_name, Video.comment_count
        FROM Video
        JOIN Channel ON Video.channel_id = Channel.channel_id
        ORDER BY Video.comment_count DESC
        LIMIT 10;
        """
        result = search_database(query)
        
        # Displaying the results in a table
        if result:
            st.write("Top 10 videos with the highest number of comments and their corresponding channels: ")
            df = pd.DataFrame(result, columns=["Video Name","Channel Name","Comment Count"])
            st.write(df)
        else:   
            st.write("No data found.")

if __name__ == '__main__':
    main()

###     ETL & Query - YouTube - Channel, Video and Comment Information

##                  Import the Relevant Packages - YouTube Data Harvesting
import googleapiclient.discovery
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

##                  Start of pre-defined functions called from the main code block

## Define a Function to Initialize youtube connection using api key
def api_connect():

    api_service_name = "youtube"
    api_version = "v3"
    api_key='AIzaSyA_EG9ouNelnuI6W597WiOGVwSCRCg5sLA'

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

    return youtube


##  Define a Function to Get Channel details using channel id
def channel_data(channel_id):

    request=youtube.channels().list(
        part="snippet,contentDetails,status,statistics",
        id=channel_id
    )
    response_1 = request.execute()

    data = {
            "channel_id"   : channel_id,
            "channel_name" : response_1['items'][0]['snippet']['title'],
            "channel_type" : response_1['items'][0]['kind'],
            "channel_viewcount" : response_1['items'][0]['statistics']['viewCount'],
            "channel_description" : response_1['items'][0]['snippet']['description'],
            "channel_status" : response_1['items'][0]['status']['privacyStatus'],
            "channel_subscriber" : response_1['items'][0]['statistics']['subscriberCount'],   
            "channel_videocount":response_1['items'][0]['statistics']['videoCount'],
            "channel_playlist_id" : response_1['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            "channel_published"  : response_1['items'][0]['snippet']['publishedAt']

    }
    global Playlist_ID
    Playlist_ID = response_1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    global Channel_Name
    Channel_Name = response_1['items'][0]['snippet']['title']
    return data


##  Define a Function to Get list of Video Ids using playlist ID through Playlist Items for the given Channel
def video_ids(channel_id):
    
    video_ids=[]
    next_page_token=None

    response_2 = youtube.playlistItems().list(
        part="snippet",
        playlistId= Playlist_ID,
        maxResults=50,
        pageToken=next_page_token).execute()

    while True:
        for i in range(len(response_2['items'])):
            video_ids.append(response_2['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token = response_2.get('nextPageToken')

        if next_page_token is None:
            break
    
    return video_ids


### Define a Function to Convert PMD/THMS format of duration into seconds - for Video duration
def duration_seconds(temp_duration):

    if 'T' not in temp_duration:
        total_duration = 0
    else:
        index_T=temp_duration.index('T')
        
        ## extract hours if present and convert to seconds
        if 'H' in temp_duration:
            index_H=temp_duration.index('H')
            H_seconds=int(temp_duration[index_T+1:index_H])*360
        else:
            H_seconds=0
        
        ## extract minutes if present and convert to seconds
        if 'M' in temp_duration:
                index_M=temp_duration.index('M')
                if 'H' in temp_duration:
                    M_seconds=int(temp_duration[index_H+1:index_M])*60
                else:
                    M_seconds=int(temp_duration[index_T+1:index_M])*60
        else:
            M_seconds = 0

        ## extract seconds if present
        if 'S' in temp_duration:
            index_S=temp_duration.index('S')
            if 'M' in temp_duration:
                S_seconds=int(temp_duration[index_M+1:index_S])
            elif 'H' in temp_duration:
                S_seconds=int(temp_duration[index_H+1:index_S])
            else:
                S_seconds=int(temp_duration[index_T+1:index_S])
        else:
            S_seconds = 0

        total_duration = H_seconds + M_seconds + S_seconds


    return total_duration


##  Define a function to Get Video Information based on Video IDs for a channel
def video_data(Video_Ids):
    video_details=[]

    for video_id in Video_Ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response_4=request.execute()

        for response_items in response_4['items']:
            temp_duration=str(response_items['contentDetails']['duration'])
            total_duration=duration_seconds(temp_duration)
            data_2 = {
                "video_id"     : response_items['id'],
                "video_name"   : response_items['snippet']['title'],
                "video_thumbnail" : response_items['snippet']['thumbnails']['default']['url'],
                "video_description" : response_items['snippet']['description'], 
                "video_pub_date" : response_items['snippet']['publishedAt'],
                "video_duration_seconds" : str(total_duration),
                "video_views"  : response_items['statistics']['viewCount'], 
                "video_comments": response_items['statistics']['commentCount'],  
                "video_favcount": response_items['statistics']['likeCount'],
                "video_definition" : response_items['contentDetails']['definition'], 
                "video_caption_status" : response_items['contentDetails']['caption'],
                "channel_name" : response_items['snippet']['channelTitle'],
                "channel_id"   : response_items['snippet']['channelId']
            }

            video_details.append(data_2)

    return video_details


###   Define a Function to get Comment Details for each video using video id
def comment_data(Video_Ids):

    comment_details=[]
    try:
        for video_id in Video_Ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100
            )

            response_5=request.execute()

            for response_items in response_5['items']:
                data = {
                    "comment_id"     : response_items['snippet']['topLevelComment']['id'],
                    "video_id"       : response_items['snippet']['topLevelComment']['snippet']['videoId'],
                    "comment_text"   : response_items['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "comment_author" : response_items['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "comment_published_date" : response_items['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comment_details.append(data)

    except:
        pass
    return comment_details
##                  End of pre-defined functions called from the main code block

##                                  MAIN CODE BLOCK

##  Initialize youtube API Connection  
youtube=api_connect()

##  Establish MySQL Connection Thread
mydb = mysql.connector.connect( host="localhost", user="root", password="",database="project_yt")
mycursor = mydb.cursor(buffered=True)

##  Create sqlalchemy engine to connect to mysql database
engine = create_engine("mysql+mysqlconnector://{user}:{password}@localhost/{mydatabase}"
                       .format(user="root",password="",mydatabase="project_yt"))

##  Create Table for Channel information
mycursor.execute('''create table IF NOT EXISTS project_yt.channel (channel_id VARCHAR(255) PRIMARY KEY,channel_name 
                 VARCHAR(255),channel_type VARCHAR(255),channel_viewcount INT, channel_description TEXT, channel_status
                 VARCHAR(255), channel_subscriber INT, channel_videocount INT, channel_playlist_id VARCHAR(255), 
                 channel_published DATETIME)''')

##  Create Table for Video information
mycursor.execute('''create table IF NOT EXISTS project_yt.video (video_id VARCHAR(255) PRIMARY KEY,video_name 
                 VARCHAR(255),video_thumbnail VARCHAR(255),video_description TEXT, video_pub_date DATETIME, 
                 video_duration_seconds INT, video_views INT, video_comments INT, video_favcount INT,
                 video_definition VARCHAR(255), video_caption_status VARCHAR(255), channel_name VARCHAR(255),
                 channel_id VARCHAR(255), FOREIGN KEY(channel_id) REFERENCES project_yt.channel(channel_id))''')

##  Create Table for Comment information
mycursor.execute('''create table IF NOT EXISTS project_yt.comment (comment_id VARCHAR(255) PRIMARY KEY,
                 video_id VARCHAR(255),comment_text TEXT,comment_author VARCHAR(255), 
                 comment_published_date DATETIME, 
                 FOREIGN KEY(video_id) REFERENCES project_yt.video(video_id))''')


##  Streamlit App Sidebar Configuration
with st.sidebar:
    st.title(':blue[YouTube Data Harvesting and Warehousing]')
    st.header('Skill Take Away')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('API Integration')
    st.caption('Data Management using Pandas and SQL')

##  Main Page Title
st.title(':blue[YOUTUBE DATA HARVESTING & WAREHOUSING]')


## Data Extraction Header
st.header(':blue[YouTube Data Extraction]')


##  User Input - Channel ID through Streamlit App
channel_id=st.text_input('Enter the Channel ID')


#                                                       DATA EXTRACTION

try:
    ##  Get channel details and convert to pandas DataFrame
    channel_details = channel_data(channel_id)
    channel_df=pd.DataFrame(channel_details,index=[0])

    ##  Get list of videos for the channel, extract video details for each video and convert to pandas DF
    Video_Ids = video_ids(channel_id)
    Video_Details = video_data(Video_Ids)
    video_df=pd.DataFrame(Video_Details)

    ##  Get all the comment details for all the vidoes in the channel and convert to pandas DF
    Comment_Details = comment_data(Video_Ids)
    comment_df=pd.DataFrame(Comment_Details)

    st.success('Channel, Video, Comment Data Successfully Extracted')
    
except KeyError:
    st.warning('Enter a Valid Channel ID')



#                                                       DATA DISPLAY
## Data Transformation Header
st.header(':blue[YouTube Data Display - After Transformation]')


##  Display the data extracted for the input channel ID
if st.button('Channel'):
    st.subheader('Channel Data Table')
    channel_df
if st.button('Videos'):
    st.subheader('Video Data Table')
    video_df
if st.button('Comments'):
    st.subheader('Comment Data Table')
    comment_df



#                                                       LOAD DATA INTO SQL DB
## Data Loading Header
st.header(':blue[YouTube Data Loading]')


##  Load the youtube data extracted into SQL Database
if st.button('Load Data into SQL Database'):
    query = '''select channel_name, channel_videocount from project_yt.channel
            order by channel_videocount desc'''
    mycursor.execute('select channel_id from project_yt.channel where channel_id=%s',[channel_id])
    mydb.commit()
    out=mycursor.fetchall()    

    if out:
        st.success('Channel Details of the given channel id already available in Database')
    else:
        channel_df.to_sql('channel',con=engine, if_exists='append', index=False)
        video_df.to_sql('video',con=engine, if_exists='append', index=False)
        comment_df.to_sql('comment',con=engine, if_exists='append', index=False)
        mydb.commit()
        st.success('All data extracted for channel, video, comment are loaded into SQL Database Successfully')


#                                                       QUERY DATA FROM DATABASE
## Data Viewing Header
st.header(':blue[YouTube Data from Database - Querying]')


##  10 pre-defined queries to view specific data from DB
input_query=st.selectbox('Query DB based on below options', ('1. All Videos and Corresponding Channels',
                                                    '2. Channel with most videos and its number',
                                                    '3. Top 10 most viewed videos and their Channels',
                                                    '4. Comment count of each Video with Channel',
                                                    '5. Videos with highest likes and their channel',
                                                    '6. Total likes for each Video Id and Video name',
                                                    '7. Number of views of each channel with name',
                                                    '8. Names of all channels which published videos in 2022',
                                                    '9. Average Duration of all videos of each channel',
                                                    '10.Videos with highest number of comments with channel'),
                                                    index=None,placeholder='Select your query')

if input_query == '1. All Videos and Corresponding Channels':
    query = '''select video_name, channel_name from project_yt.video'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df1=pd.DataFrame(out,columns=["Video Name","Channel Name"])
    st.write(df1)

if input_query == '2. Channel with most videos and its number':
    query = '''select channel_name, channel_videocount from project_yt.channel
            order by channel_videocount desc'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df2=pd.DataFrame(out,columns=["Channel Name", "No of Videos"])
    st.write(df2)

if input_query == '3. Top 10 most viewed videos and their Channels':
    query = '''select video_name, video_views, channel_name from project_yt.video
            where video_views is not null
            order by video_views desc limit 10'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df3=pd.DataFrame(out,columns=["Video Name","Video View Count","Channel Name"])
    st.write(df3)

if input_query == '4. Comment count of each Video with Channel':
    query = '''select video_name, video_comments from project_yt.video
            where video_comments is not null'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df4=pd.DataFrame(out,columns=["Video Name","Total No of Comments"])
    st.write(df4)

if input_query == '5. Videos with highest likes and their channel':
    query = '''select video_name , channel_name , video_favcount from project_yt.video
            where video_favcount is not null
            order by video_favcount desc'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df5=pd.DataFrame(out,columns=["Video Name","Channel Name","Likes"])
    st.write(df5)

if input_query == '6. Total likes for each Video Id and Video name':
    query = '''select video_id, video_name, video_favcount from project_yt.video
            where video_favcount is not null
            order by video_favcount desc'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df6=pd.DataFrame(out,columns=["Video Id","Video Name","Likes"])
    st.write(df6)

if input_query == '7. Number of views of each channel with name':
    query = '''select channel_id, channel_name, channel_viewcount from project_yt.channel'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df7=pd.DataFrame(out,columns=["Channel Id","Channel Name","No of Views"])
    st.write(df7)

if input_query == '8. Names of all channels which published videos in 2022':
    query = '''select video_name, video_pub_date, channel_name from project_yt.video
            where extract(year from video_pub_date)=2022'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df8=pd.DataFrame(out,columns=["Video Name","Video Release Date","Channel Name"])
    st.write(df8)

if input_query == '9. Average Duration of all videos of each channel':
    query = '''select channel_name, avg(video_duration_seconds) as AverageDuration from project_yt.video
            group by channel_name'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df9=pd.DataFrame(out,columns=["Channel Name","Average Video Duration in seconds"])
    st.write(df9)

if input_query == '10.Videos with highest number of comments with channel':
    query = '''select video_name, channel_name, video_comments from project_yt.video
            where video_comments is not null
            order by video_comments desc'''
    mycursor.execute(query)
    mydb.commit()
    out=mycursor.fetchall()
    df10=pd.DataFrame(out,columns=["Video Name","Channel Name","Total No of Comments"])
    st.write(df10)


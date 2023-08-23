
import streamlit as st
from googleapiclient.discovery import build
## import pymongo as pg
import pymongo

# To use it correctly, you need to import the datetime class from the datetime module
from datetime import datetime
import time

import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

st.set_page_config(page_title="Youtube Data Harvesting", layout="wide", initial_sidebar_state="auto", menu_items=None)

st.title(":green[YouTube Data Harvesting] ")

st.subheader(":blue[Fetch Youtube Data and Upload to a MongoDB Database] ")


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set up the API client
api_key = "AIzaSyDF_rrgB_siisonsTUvtGMmYgy7nF3y0rc"
youtube = build('youtube', 'v3', developerKey=api_key)

## MIT OpenCourseware    channel_id=UCEBb1b_L6zDS3xTUrIA
channel_id = "UCEBb1b_L6zDS3xTUrIA "     ## MIT OpenCourseware

##   Harvard University CS50 Channel Id    ==>     channel_id=UCcabW7890RKJzL968QWEyk
channel_id = "UCcabW7890RKJzL968QWEykA"  ## Harvard University CS50 Channel Id


## channel_name = st.text_input("Enter the channel Name")
channel_id = st.text_input("Enter the channel Id")
submit = st.button("Fetch Youtube Channel details and Transfer data to Mongodb")

if submit:  ## if the button "Fetch Youtube Channel details and Transfer data to Mongodb" was pressed?
    # Get the channel ID
    ##  channel_name = "PewdiePie"
    ## request = youtube.search().list(q=channel_name, type='channel', part='id', maxResults=1)
    ## response = request.execute()
    ## channel_id = response['items'][0]['id']['channelId']

    # Use the channel ID to get the channel statistics
    request = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id)
    response = request.execute()

    # Print the statistics
    ## st.write(f"Channel name: {channel_name}")
    st.write(f"Channel ID: {channel_id}")
    st.write("Channel Id: " + response['items'][0]["id"])
    st.write("Channel Name: " + response['items'][0]["snippet"]["title"])
    st.write("Channel Description : " + response['items'][0]["snippet"]["description"])
    st.write("View Count: " + response['items'][0]['statistics']['viewCount'])
    st.write("Subscriber Count: " + response['items'][0]['statistics']['subscriberCount'])
    st.write("Video Count: " + response['items'][0]['statistics']['videoCount'])
    ##  st.write(response['items'][0])
    

@st.cache_data
def get_all_videos_in_channel(channel_id):

    all_videos = []
    
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id=channel_id)
    response =  request.execute()
    
    for item in response["items"]:
        ## print(response["items"])
        data = {
                   "channel_id"        :  item["id"],
                   "channel_name"      : item["snippet"]["title"],
                   "subscriber_count"  : item["statistics"]["subscriberCount"],
                   "channel_views"     : item["statistics"]["viewCount"] ,
                   "total_videos"      : item["statistics"]["videoCount"] ,
                   "playlist_id"       : item["contentDetails"]["relatedPlaylists"]["uploads"],
                   "description"       : item["snippet"]["description"],
                   "published_date"       : item["snippet"]["publishedAt"],
                   "view_count"         : item["statistics"]["viewCount"],
               }
        all_videos.append(data)
        
        return (all_videos)
        ## return (pd.DataFrame(all_data))

    
@st.cache_data
def get_all_video_ids(_youtube,playlist_id_data):
    list_of_all_video_ids = []

    for i in playlist_id_data:
        next_page_token = None
        more_pages = True
        ## print(i)

        while more_pages:
            request = youtube.playlistItems().list(
                        part = 'snippet, contentDetails',
                        playlistId = i,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
            
            for j in response["items"]:
                list_of_all_video_ids.append(j["contentDetails"]["videoId"])
        
            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                more_pages = False
    return list_of_all_video_ids



@st.cache_data
def get_all_video_details(_youtube,video_id):

    all_video_stats = []

    for i in range(0,len(video_id),50):
        
        request = youtube.videos().list(
                  part="snippet,contentDetails,statistics",
                  id = ",".join(video_id[i:i+50]))
        response = request.execute()
        
        for video in response["items"]:
            published_dates = video["snippet"]["publishedAt"]
            parsed_dates = datetime.strptime(published_dates,'%Y-%m-%dT%H:%M:%SZ')
            format_date = parsed_dates.strftime('%Y-%m-%d')

            videos = dict( channel_id = video["snippet"]["channelId"], 
                           video_id = video["id"],
                           video_name = video["snippet"]["title"],
                           published_date = format_date ,
                           view_count = video["statistics"].get("viewCount",0),
                           like_count = video["statistics"].get("likeCount",0),
                           comment_count= video["statistics"].get("commentCount",0),
                           duration = video["contentDetails"]["duration"])
            all_video_stats.append(videos)

    ## return pd.DataFrame(all_video_stats)
    return (all_video_stats)

@st.cache_data
def get_all_comments(_youtube,video_ids):
    comments_data= []
    try:
        next_page_token = None
        for i in video_ids:
            while True:
                request = youtube.commentThreads().list(
                    part = "snippet,replies",
                    videoId = i,
                    textFormat="plainText",
                    maxResults = 100,
                    pageToken=next_page_token)
                response = request.execute()

                for item in response["items"]:
                    published_date= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    parsed_dates = datetime.strptime(published_date,'%Y-%m-%dT%H:%M:%SZ')
                    format_date = parsed_dates.strftime('%Y-%m-%d')
                    

                    comments = dict(
                                    channel_id = channel_id,
                                    video_id = item["snippet"]["videoId"],
                                    comment_id = item["id"],
                                    comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                    comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    comment_published_date = format_date)
                    comments_data.append(comments) 
                
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break       
    except Exception as e:
        print("Inside function get_all_comments, in Try block : An error occured",str(e))          
            
    return comments_data
##    return pd.DataFrame(comments_data)


if submit:  ## if the button "Fetch Youtube Channel details and Transfer data to Mongodb" was pressed?
    channel_list = [channel_id]
    all_channel_ids = get_all_videos_in_channel(channel_id)
    print("Details of the Channel as follows")
    print(all_channel_ids)   ## will also display the playlist id, which needs to be noted for next step

    ch_play_list_ids = all_channel_ids[0]["playlist_id"]

    ## Debugging code start ##
    channel_id_1     = all_channel_ids[0]["channel_id"]
    ch_name          = all_channel_ids[0]["channel_name"]
    ch_subsc         = all_channel_ids[0]["subscriber_count"]
    ch_desc          = all_channel_ids[0]["description"]
    ch_play_list_ids = all_channel_ids[0]["playlist_id"]

    print ("Channel Id :" + channel_id_1)
    print ("Channel Name :" + ch_name)
    print ("Subscription :" + ch_subsc)
    print ("Description :" + ch_desc)
    print ("Playlist_id :" + ch_play_list_ids)

    ## Debugging code end ##

    playlist_id = list(ch_play_list_ids.split(" "))

    video_ids = get_all_video_ids(youtube, playlist_id)


    ## Debugging code start ##
    print(playlist_id)
    print(video_ids)
    ## Debugging code end ##

    ## Display the video details for ONE video
    ## video_df = pd.DataFrame(get_all_video_details(youtube, video_ids))  
    video_datas = get_all_video_details(youtube, video_ids)  
    print(video_datas)

    ## Each video will have several comments, pickup all comments for a video and display it.
    ## get comments for each video listed above
    comments = get_all_comments(youtube,video_ids)
    print(comments)


## Mongo db connection at the Global scope
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb   = client["YoutubeDataHarvest"]
mycol1 = mydb["channel_data"]
mycol2 = mydb["video_data"]
mycol3 = mydb["comment_data"]

#### submit1 = st.button("Fetch Channel details from Youtube and Upload to MongoDB Database")
if submit:

    ###   Start ==> Code block to delete all data in Mongodb while doing the testing
    ## Delete All Documents in the mongodb Collection
    ## mongodo ==> dbname ==> YoutubeDataHarvest
    import pymongo

    ## client = pymongo.MongoClient("mongodb://localhost:27017/")
    ## mydb = client["YoutubeDataHarvest"]

    ## mycol1 = mydb["channel_data"]
    ## mycol2 = mydb["video_data"]
    ## mycol3 = mydb["comment_data"]

    ## x = mycol1.delete_many({})    ## Delete all rows from channel_data ==> Only on debugging
    ## y = mycol2.delete_many({})    ## Delete all rows from video_data   ==> Only on debugging
    ## z = mycol3.delete_many({})    ## Delete all rows from comment_data ==> Only on debugging

    ## print(x.deleted_count, " Channels deleted.") ## Debugging code start ##
    ## print(y.deleted_count, " Videos deleted.")
    ## print(z.deleted_count, " Comments deleted.") ## Debugging code end ##

    ## print(all_channel_ids) ## Debugging code start ##
    ## print(video_datas)
    ## print(comments) ## Debugging code end ##
    ## client.close()
    ###   End ==> Code block to delete all data in Mongodb while doing the testing


    ## Insert data into mongo db ==> dbname ==> YoutubeDataHarvest

    if all_channel_ids:
        mycol1.insert_many(all_channel_ids)
    if video_datas:
        mycol2.insert_many(video_datas)
    if comments:
        mycol3.insert_many(comments)

    ## Debugging code start ##
    ## Display mongodb data, just to check if Data is stored correctly
    ## client = pymongo.MongoClient("mongodb://localhost:27017/")
    ## mydb   = client["YoutubeDataHarvest"]
    ## mycol1 = mydb["channel_data"]
    ## mycol2 = mydb["video_data"]
    ## mycol3 = mydb["comment_data"]

    print("All Channel Details")
    for x in mycol1.find():
        print(x)

    print("All Video Data")
    for x in mycol2.find():
        print(x)
    
    print("All Comments Data")
    for x in mycol3.find():
        print(x)
    
    ## Debugging code end ##


def get_all_channel_names():   
    ch_name = []
    for i in mydb.channel_data.find():
        ch_name.append(i['channel_name'])
    return ch_name


if submit:
    all_channel_names = get_all_channel_names()
    print(all_channel_names)           ## Debugging code
    print(all_channel_names[0])        ## Debugging code


def get_channel_details(user_input):
    ## GP commented the 4 lines below since in operator fails to bring in data
    ## no time for debugging, so changed the find approach, Apr-21-2023
    
    ## query = {"channelName":{"$in":list(user_input)}}
    query = {"channel_name":{"$in":list(user_input)}}
    projection = {"_id":0,"channel_id":1,"channel_name":1,"channel_views":1,"subscriber_count":1,"total_videos":1,"playlist_id":1}
    x = mycol1.find(query,projection)
    channel_table = pd.DataFrame(list(x))
    return channel_table
    
    ## GP added 6 lines below, since "$in" function above did not work Apr-21-2023
    ## channel_table = []
    ## myquery={'channel_name':user_input}
    ## for each in mycol1.find(myquery):
    ## for each in mycol1.find(myquery,projection):
        ## print(each)
        ## channel_table.append(each)
    ## return pd.DataFrame(channel_table)
    

#Fetching Video details:
def get_video_details(channel_list):
    query = {"channel_id":{"$in":channel_list}}
    myquery={'channel_id':channel_list}   ## GP Added 
    projection = {"_id":0,"video_id":1,"channel_id":1,"video_name":1,"published_date":1,"view_count":1,"like_count":1,"comment_count":1,"duration":1}
    x = mycol2.find(query,projection)  ## GP Commented
    ## x = mycol2.find(myquery,projection)   ## GP Added
    video_table = pd.DataFrame(list(x))
    return video_table



#Fetching Comment details:
def get_comment_details(video_ids):
    query = {"video_id":{"$in":video_ids}}
    myquery={'video_id':video_ids}   ## GP Added 
    projection = {"_id":0,"comment_id":1,"video_id":1,"comment_text":1,"comment_author":1,"comment_published_date":1}
    x = mycol3.find(query,projection)       ## GP Commented
    ## x = mycol3.find(myquery,projection)        ## GP Added
    comment_table = pd.DataFrame(list(x))
    return comment_table


st.subheader(":orange[Select the Channel Name to Insert into the MySQL for later Analysis] ⌛")
   
user_input =st.multiselect("Select channel Name, whose details will be Transfered to MySQL Data",options = get_all_channel_names())


if submit:


    ## Debugging code start ##

    ## Search and print the data from mongodb collections (tables)

    ## checks whether the list user_input is empty or not
    ## This is achieved by comparing the list to an empty list using the inequality operator !=

    if user_input != []:    ## The list is not empty
        print("Finding channel_name for user_input = " + user_input[0])

    ## Search mongodb using the in operator
    myquery={"channel_name":{"$in":list(user_input)}}
    mydoc = mycol1.find(myquery)

    print("The channel details using in operator")
    for x in mydoc:
        print(x)
    
    ## Search mongodb using the normal find operator
    print("The channel details using normal find function ")
    myquery={'channel_name':user_input}
    for each in mycol1.find(myquery):
        print(each)

    ## Debugging code end ##


submit2 = st.button("Upload Channel details data into MySQL")

if submit2:

    if user_input != []:    ## The list is not empty
        print("Finding channel_name for user_input = " + user_input[0])
    else:
        st.error("Please select atleast one Channel name from the list")

    
    if user_input != []:    ## The list is not empty
        channel_data = get_channel_details(user_input)
        ## Debugging code start ##
        print("After calling get_channel_details function : channel_data = ")
        print(channel_data)
        ## print("Channel Name :" + channel_data['channel_name'][0])
        ## print("Channel Subscription :" + channel_data['subscriber_count'][0])
        ## print("Channel Playlist id :" + channel_data['playlist_id'][0])
        ## Debugging code end ##


        channel_list = [channel_id]
        ## Fetch video_ids from mongoDb
        video_data = get_video_details(channel_list)
        print(" After calling get_video_details function : video_data = ")
        print(video_data)


        ## Fetch video_ids from mongoDb
        video_ids = video_data["video_id"].to_list()
        
        ## Fetch comments data from mongoDb
        comment_data = get_comment_details(video_ids)
        ##st.write(comment_data)
        print(" After calling get_comment_details function : comment_data = ")
        print(comment_data)        
        ## mydb.close()
        client.close()


        connection = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = "testpass",
            database = "youtube_data_warehousing",
auth_plugin='mysql_native_password'
         )

        cursor = connection.cursor()

        #Creating an SQLAlchemy engine to connect to the database:
        engine = create_engine('mysql+mysqlconnector://root:mysql007@localhost/youtube_data_warehousing')

    # Inserting Channel data into the table using try and except method:
        try:
            # Attempt to insert the data
            channel_data.to_sql('channel_data', con=engine, if_exists='append', index=False, method='multi')
            st.write("channel_data inserted successfully")
        except Exception as e:
            if 'Duplicate entry' in str(e):
                st.write("Duplicate data found. Ignoring duplicate entries.")
            else:
                st.write("An error occurred:", e)


    # Inserting Video data into the table using try and except method:

        try:
            # Attempt to insert the data
            video_data.to_sql('video_data', con=engine, if_exists='append', index=False, method='multi')
            st.write("video_data inserted successfully")
        except Exception as e: 
            if 'Duplicate entry' in str(e):
                st.write("Duplicate data found. Ignoring duplicate entries.")
            else:
                st.write("An error occurred:", e)
        ## st.success("Data Uploaded Successfully")

        engine.dispose()

        # Inserting comment data into the table using try and except method:

        try:
            # Attempt to insert the data
            comment_data.to_sql('comment_data', con=engine, if_exists='append', index=False, method='multi')
            st.write("comment_data inserted successfully")
        except Exception as e: 
            if 'Duplicate entry' in str(e):
                st.write("Duplicate data found. Ignoring duplicate entries.")
            else:
                st.write("An error occurred:", e)
        st.success("Data Transfered to MySQL Successfully")

        engine.dispose()


st.subheader(":green[Let us get some Insights, Select the question below?]")

#MySQL Database Connection:

connection = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = "testpass",
            database = "youtube_data_warehousing",
            auth_plugin='mysql_native_password'
       )

cursor = connection.cursor()
  


questions = st.selectbox("Select any questions given below:",
['Select the question that you would like to get answers',
'1. Name all videos and their channels?',
'2. Channels with most number of videos, and their number of videos?' ])

if questions == '1. Name all videos and their channels?' :
    query1 = "select channel_name as Channel_name ,video_name as Video_names from channel_data c join video_data v on c.channel_id = v.channel_id;"
    cursor.execute(query1)
    #Storing the results in Pandas Dataframe:
    result = cursor.fetchall()
    table1 = pd.DataFrame(result,columns = cursor.column_names)
    st.table(table1)
elif questions == '2. Channels with most number of videos, and their number of videos?' :
    query2 = "select channel_name,count(video_name) as Most_Number_of_Videos from video_data v join channel_data c on c.channel_id = v.channel_id group by channel_name order by count(video_name) desc;"
    cursor.execute(query2)
    result1 = cursor.fetchall()
    table2 = pd.DataFrame(result1,columns =cursor.column_names)
    st.table(table2)
    st.bar_chart(table2.set_index("channel_name"))


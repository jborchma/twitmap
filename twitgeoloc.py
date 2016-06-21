from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import pandas as pd
import json
import time, sys
import sqlite3
import datetime

# I can set up the program as follows: I have three tables in my database, events, eventsTemp and eventsLog. Every time I run my program, I will write 
# new data into eventsTemp and create an eventsLog entry. The next time, the program runs, I will move the eventsTemp entries into events and run the program anew.
# This is due to the fact, that I have to cancel the streaming by hand. Maybe if I can find out how I can limit the stream, I can change this.


# Twitter access keys
consumer_key=''
consumer_secret=''
access_token=''
access_token_secret=''

##### Create the SQL database

def create_tables():

    conn = sqlite3.connect("geodata.db")

    with conn:
        # select a dictionary cursor
        # conn.row_factory = sqlite3.Row
        
        cursor = conn.cursor()

        # table for the twitter entries is created if it not exists
        cursor.execute("""CREATE TABLE if not exists events
                  (id integer primary key not null, service text, user text, time integer, lang text,
                   tweet text, location text) """)

        # conn.commit() # don't need the commit statement when using with

        # temp table for safeguarding against error. it's the same as the events table
        cursor.execute("""CREATE TABLE if not exists eventsTemp
                  (id INTEGER primary key not null, service TEXT, user TEXT, time INTEGER, lang TEXT,
                   tweet TEXT, location TEXT) """)

        # evnetslog table to keep a record when we collect data
        cursor.execute("""CREATE TABLE if not exists eventsLog
                  (BatchID INTEGER primary key not null, RunDate TEXT null, Service TEXT null, HarvestedThisRun INTEGER null,
                   TotalHarvested INTEGER null) """)

        # this query prints the names of all the tables in the database
        # cursor.execute("""SELECT name FROM sqlite_master WHERE type='table' ORDER BY name""")
        # print(cursor.fetchall())

def stream_data():

    conn = sqlite3.connect("geodata.db")
        
    l = StdOutListener() #this will be the class that saves the tweet into the temp database
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

        #This line filter Twitter Streams to capture data by the location
    stream.filter(locations=[-79.56,43.5,-79.15,43.83]) #Toronto/GTA


def copy_data():

    conn = sqlite3.connect("geodata.db")
    cursor = conn.cursor()

    with conn:

        cursor.execute("SELECT max(batchid) FROM eventslog")
        
        # Batch ID generated
        batch_id_cur = cursor.fetchall()
        if batch_id_cur[0][0] == None:
            batch_id = 0
        else:
            batch_id = batch_id_cur[0][0]+1

        print('Batch ID:', batch_id)

        #Copy data from Temp to event

        cursor.execute(""" SELECT * FROM eventsTemp """)
        print('Events in Temp: ', len(cursor.fetchall()))

        cursor.execute(""" SELECT * FROM events """)
        print('Events in Events: ', len(cursor.fetchall()))

        cursor.execute("""INSERT INTO events SELECT * FROM eventsTemp WHERE eventsTemp.id NOT in (SELECT distinct events.id FROM events)""")
        cursor.execute("""DELETE FROM eventsTemp""")
        cursor.execute(""" SELECT * FROM eventsTemp """)
        print('Events in Temp: ', len(cursor.fetchall()))

        cursor.execute(""" SELECT * FROM events """)
        print('Events in Events: ', len(cursor.fetchall()))

        # get date
        now = datetime.datetime.now()
        date_today = str(now.strftime("%Y-%m-%d %H:%M"))

        # Get number of events
        cursor.execute("""SELECT count(*) FROM events""")

        #df = pd.DataFrame(cursor.fetchall(), columns=['id','service','user','time','lang','tweet','location'])
        event_sum = cursor.fetchall()

        if not event_sum:
            event_sum = 0

        total = int(event_sum[0][0])
        cursor.execute("""SELECT ifnull(TotalHarvested,0) FROM eventsLog ORDER BY RunDate DESC LIMIT 1""")
        old_total_harvested = cursor.fetchall()
        if not old_total_harvested:
            old_total_harvested = 0
        else:
            old_total_harvested = old_total_harvested[0][0]

        harvestedthisrun = int(total - old_total_harvested)
        print('Harvested this run:', harvestedthisrun)

        if harvestedthisrun > 0:
            cursor.execute("""INSERT INTO eventsLog(BatchID,RunDate,Service,HarvestedThisRun,TotalHarvested) VALUES (?,?,?,?,?)""", (batch_id, date_today, 'twitter',harvestedthisrun,total ))


# all the keys of a tweet dict: 
# dict_keys(['source', 'in_reply_to_user_id', 'favorite_count', 'text', 'in_reply_to_status_id', 
# 'filter_level', 'created_at', 'id', 'lang', 'coordinates', 'truncated', 'id_str', 'favorited', 
# 'timestamp_ms', 'user', 'retweeted', 'in_reply_to_status_id_str', 'in_reply_to_user_id_str', 'place', 
# 'entities', 'is_quote_status', 'in_reply_to_screen_name', 'geo', 'retweet_count', 'contributors'])

# this prints the data that I get from the twitter stream, either there is data or there is an error
class StdOutListener(StreamListener):

    def on_data(self, data):
        tweet = json.loads(data)

        new_time = float(int(tweet['timestamp_ms']))/1000

        # if tweet['coordinates'] == None: 
        #     if tweet['place']['country'] == 'Canada':    
        #         print('Canadian tweet without coordinates')
        if tweet['coordinates'] != None:
            conn = sqlite3.connect("geodata.db")
            cursor = conn.cursor()

            #-79.56,43.5,-79.15,43.83
            # we need to filter out faulty coordinates (most of them in New York)
            if tweet['coordinates']['coordinates'][0] > -79.56 and tweet['coordinates']['coordinates'][0] < -79.15:

                # this will write the data to the temp table
                with conn:
                    coord = str(tweet['coordinates']['coordinates'][0]) + ',' + str(tweet['coordinates']['coordinates'][1])
                    print('Found tweet at:',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(new_time)))
                    print(coord)
                    try:
                        identity = int(tweet['id'])
                        service = 'twitter'
                        user = str(tweet['user']['screen_name'])
                        timestamp = int(tweet['timestamp_ms'])
                        lang = str(tweet['lang'])
                        tweet = str(tweet['text'])
                        cursor.execute(""" INSERT INTO eventsTemp(id,service,user,time,lang,tweet,location) VALUES (?,?,?,?,?,?,?)""", (identity,service,user,timestamp,lang,tweet,coord) )
                    except Exception as e:
                        raise e
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':

    create_tables()
    copy_data()
    stream_data()

    # conn = sqlite3.connect("geodata.db")
    # cursor = conn.cursor()

    # with conn:
    #     cursor.execute("""SELECT * FROM eventsLog """)
    #     print(cursor.fetchall())


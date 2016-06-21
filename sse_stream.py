import folium
import sqlite3
import pandas as pd
import time
import signal
import sys
import json
from flask import Flask
from flask import request
from flask import abort
from flask import url_for
from flask import make_response
from flask import Response


# one can find the map under http://localhost:5000/static/map.html

bbox = [43.5,-79.56,43.83,-79.15]
map_osm = folium.Map(location=[43.665,-79.355],zoom_start=12)
#map_osm.fit_bounds(bbox)

conn = sqlite3.connect("geodata.db")
cursor = conn.cursor()

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)

# cursor.execute(""" SELECT location FROM events WHERE location NOT LIKE '%79.%' """)
# print(cursor.fetchall())
def sql_tail():
    number_of_points_old = 0
    while True:
        conn = sqlite3.connect("geodata.db")
        cursor = conn.cursor()
        #map_osm = folium.Map(location=[43.665,-79.355],zoom_start=12)
        cursor.execute(""" SELECT location, service, tweet FROM eventsTemp """)
        data = pd.DataFrame( cursor.fetchall(),columns=['location','service','tweet'])
        number_of_points = data.shape[0]

        #m = folium.Marker(location=[43.665,-79.355], popup='test',icon=folium.Icon(color='blue',icon='cloud'))

        #map_osm.add_children(m)


        # for i in range(number_of_points_old): #data.shape[0]
        #     loc = [float(x) for x in data.iloc[i,0].split(',')]
        #     loc = loc[::-1]
        #     loc_dict = json.loads({'coordinates':loc})
        #     serv = data.iloc[i,1]
        #     tweet = data.iloc[i,2]
        #     popup_text = serv + ': ' + tweet
        #     #print(loc)
        #     yield 'data: %s\n\n' % loc_dict


        for i in range(number_of_points_old,data.shape[0]): #data.shape[0] 
            loc = [float(x) for x in data.iloc[i,0].split(',')]
            loc = loc[::-1]
            serv = data.iloc[i,1]
            tweet = data.iloc[i,2]
            popup_text = tweet
            # print('data: %s\n\n' % loc)
            #yield 'data: %s\n\n' % lo
            yield "data: " + str(loc[0]) + "," + str(loc[1]) + "," + str(tweet) + "\n\n"

        number_of_points_old = number_of_points
        #map_osm.save('test.html')
        print('##### Refreshed #####')
        time.sleep(5)


app = Flask(__name__)
@app.route('/tweets')
def tweets():
   
    url_for('static', filename='map.html')
    url_for('static', filename='jquery-1.7.2.min.js')
    url_for('static', filename='jquery.eventsource.js')
    url_for('static', filename='jquery-1.7.2.js')
    return Response(sql_tail(), headers={'Content-Type':'text/event-stream'})

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    app.run(debug=True, host='0.0.0.0') 



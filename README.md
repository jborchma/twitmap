Twitmap
==========

This is a short intro in the twitmap program that collects tweets via the twitter API and sends them via server side events to a simple interactive map of Toronto.

In order to start the program, clone it from this depository and add the necessary twitter api credential in twitgeoloc.py. Then, run both twitgeoloc.py and sse_stream.py and the map including a simple log should be accessible at http://localhost:5000/static/map.html

Function of the program
-------------------------

The program is set up in the following way:

- first, any entries in the old temp table of the geodata.db are transfered into the full table for tweets
- twitgeoloc.py accesses the twitter API and filters the data stream depending on the GPS location of the tweet
- the tweet is then saved in a temp table in the geodata.db
- the number of the new tweets is logged in a log table in the database
- sse_stream accesses the temp table of the database and creates a server side event stream via flask which will be read by the map.html file
- the map.html file creates the map that will be displayed at http://localhost:5000/static/map.html including a simple log

Future extensions
--------------------

So far, this program only displays the tweets on a map. Now, the data should be analyzed:

- classify tweets by topic and or language 
- analyze predictive power of tweets by location
- add other social media services that have location data



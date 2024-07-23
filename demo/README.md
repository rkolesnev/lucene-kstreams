###Build and run

Create and activate a python virtual env and install requirements7
``````commandline
python3 -m venv venv && . venv/bin/activate && pip install -r requirements.txt
``````
Start up confluent platform with docker
```commandline
docker-compose up -d
```

Start GeoKStream java application - Note - Java 16 is required.
```commandline
java -jar lucene-kstreams-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Produce taxi locations to kafka
```commandline
python produce_taxis.py
```

Taxi monitoring web app - open a new terminal and start the webserver to stream taxi events over http
```commandline
python app_taxis.py
```

Visit http://localhost:6002/ from a browser to view the taxis moving on the map.

Click on the map to see which taxis should be captured.


Taxi hauling web app - open a new terminal and start the webserver to stream taxi events over http
```commandline
python app_people.py
```

Visit http://localhost:6001/ from a browser to view the map.
Click on the map to request a taxi.

###Clean up:

Stop the producers, GeoKStream app and web app by `Ctrl-C` In each of the terminals running `produce_taxis.py` and `app.py` and `java GeoKstreams app`

Stop confluent platform:
```commandline
docker-compose down
```



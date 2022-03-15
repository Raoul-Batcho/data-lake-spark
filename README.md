## Sparkify-data-lake project

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in "data/log_data/" a directory of JSON logs on user activity on the app, as well as "data/song_data" a directory with JSON metadata on the songs in their app.

We are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Schema for Song Play Analysis
Using the song and log datasets, we create a star schema optimized for queries on song play analysis. This includes the following tables.

  ### Fact Table
        songplays - records in log data associated with song plays i.e. records with page NextSong
                songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
  ### Dimension Tables
        users - users in the app
                user_id, first_name, last_name, gender, level
        songs - songs in music database
                song_id, title, artist_id, year, duration
        artists - artists in music database
                artist_id, name, location, lattitude, longitude
        time - timestamps of records in songplays broken down into specific units
               start_time, hour, day, week, month, year, weekday

### Files
   #### etl.py  
        Contains script which copies data to enviroment for transformation then inserts data into dimension tables on S3
        contains four functions:
        
            1- create_spark_session(): creates and returns spark session 
            
            2- process_song_data(): Takes three parameters. 
                           spark: spark session to read song data from S3, processes that data using Spark, and writes them back to S3.
                           input_data: Contains S3 location to read log data from. 
                           output_data: Contains S3 location to write data back to.
            
            3- process_log_data(): Takes three parameters. 
                           spark: spark session to reads log data from S3, processes that data using Spark, and writes them back to S3.
                           input_data: Contains S3 location to read log data from. 
                           output_data: Contains S3 location to write data back to.
                           
            4- main():
                  The main function which executes the ETL processes through the above functions.
                  
   #### dl.cfg 
        Stores AWS Login Credentials.
   
### Instructions
   1- Create or navigate to an AWS S3 bucket and add the required aws login credentials to the dl.cfg file.
   
   2- Type "run etl.py" in terminal to start the ETL process

### Starting the program
    1- Add global aws config values in dl.cfg
    
    2- Execute "etl.py".

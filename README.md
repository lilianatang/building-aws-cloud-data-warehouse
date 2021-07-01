# INTRODUCTION
### Purpose of the project:
This project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 
### What is Sparkify?
Sparkify is a startup that has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
### How this project is going to help Sparkify
The data pipeline will allow the analytics team to continue finding insights in what songs their users are listening to. 
# DATABASE SCHEMA DESIGN & ETL PROCESS
### Database Schema Design (Star Schema)
##### **Fact Table**
songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)** - records in log data associated with song plays i.e. records with page NextSong
##### **Dimension Tables**
1. users (user_id, first_name, last_name, gender, level) - users in the app
2. songs (song_id, title, artist_id, year, duration) - songs in music database
3. artists (artist_id, name, location, latitude, longitude) - artists in music database
4. time (start_time, hour, day, week, month, year, weekday) - timestamps of records in **songplays** broken down into specific units
### ETL Process
1. Create fact and dimension tables for the star schema in Redshift.
2. Load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
# FILES IN REPOSITORY
* etl.py loads data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
* create_table.py creates fact and dimension tables for the star schema in Redshift.
* sql_queries.py defines SQL statements, which will be imported into the two other files above.
* README.md provides discussion on the project
# HOW TO RUN THE PYTHON SCRIPTS
* Run create_tables.py to create table schemas.
* Run python etl.py to read song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.

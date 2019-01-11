# Overview
This project was designed using Airflow, Spark, AWS S3 Buckets, Snowflake and Python. It ingests data from a mySQL database. The [wiki](https://github.com/RCS-Training-12-18/Retail-Case-Study/wiki) has the original document given as the project. 
# Setup
The bucket location is hardcoded into the code. This can be changed to use an airflow variable and passed in if airflow is setup and stable. MySQL credentials are saved in a file, but also can be set up as airflow variables. If the MySQL credentials are added as variables, the `mysql_creds()` function in part1, needs to be changed to return the variables. 
## A Fresh WSL
If you are starting fresh. Install a windows linux subsystem (This project was tested with both WSLs running Ubuntu 16.04 and Ubuntu 18.04). Run the bash script in the Script folder. This will install all needed packages and set up airflow. To run this project, install MySQL when it prompts at the end of the install.  
## Startup Script
Running the startup.bash will set up the tables with the new **last_update** column. If the database isn't named foodmart, change that line in the script.
## The Dag
Move the dag into an appropriate folder as defined by your airflow.cfg. 
## Tables
The tables file is meant to be in a folder called **config_files** in the S3 bucket. If tables are added to the list also add them to the startup script and rerun it to addthe **last_update** column.
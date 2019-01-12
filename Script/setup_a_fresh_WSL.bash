#!/bin/bash
sudo apt install -y spark
sudo apt-get update
sudo apt-get install -y default-jdk
sudo apt install -y python2.7
sudo apt install -y python-pip
pip install --upgrade pip
cp .bashrc .oldbashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin' >> .bashrc
. .bashrc
#MySQL
sudo apt-get install -y mysql-server
sudo service mysql start
# postgres
sudo apt-get install -y postgresql postgresql-contrib
sudo service postgresql start
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'airflow';"
sudo -u postgres createdb airflow
# airflow
sudo apt-get install -y libmysqlclient-dev
sudo apt-get install -y libssl-dev
sudo apt-get install -y libkrb5-dev
sudo apt-get install -y libsasl2-dev
sudo AIRFLOW_GPL_UNIDECODE=yes ~/.local/bin/pip install apache-airflow
#update airflow config
airflow initdb
cp airflow/airflow.cfg airflow/oldairflow.cfg
sed -i 's?sql_alchemy_conn = sqlite:////home/'$USER'/airflow/airflow.db?sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow?' airflow/airflow.cfg
sed -i 's/executor = SequentialExecutor/executor = LocalExecutor/' airflow/airflow.cfg
rm airflow/airflow.db
airflow initdb
pip install boto3
sudo apt-get install -y libssl-dev libffi-dev
sudo ~/.local/bin/pip install --upgrade snowflake-connector-python
clear
read -s -p 'Enter the password you want for your MySQL root user' pw
echo ''
sudo mysql <<EOF
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password by '$pw';
flush privileges;
EOF
#clear 
echo 'Installation Complete'

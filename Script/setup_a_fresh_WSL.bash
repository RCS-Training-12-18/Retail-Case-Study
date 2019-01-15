#!/bin/bash
sudo apt install -y spark
sudo apt-get update
sudo apt-get install -y default-jdk
sudo apt install -y python2.7
sudo apt install -y python-pip
pip install --upgrade pip
cp ~/.bashrc ~/.oldbashrc
echo 'export JAVA_HOME=/usr/lib/jvm/default-java
export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
. ~/.bashrc
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
sudo AIRFLOW_GPL_UNIDECODE=yes ~/.local/bin/pip install apache-airflow[all]
#update airflow config
airflow initdb
cp ~/airflow/airflow.cfg ~/airflow/oldairflow.cfg
sed -i 's?sql_alchemy_conn = sqlite:////home/'$USER'/airflow/airflow.db?sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow?' ~/airflow/airflow.cfg
sed -i 's/executor = SequentialExecutor/executor = LocalExecutor/' ~/airflow/airflow.cfg
rm ~/airflow/airflow.db
airflow initdb
pip install boto3
sudo apt-get install -y libssl-dev libffi-dev
sudo ~/.local/bin/pip install --upgrade snowflake-connector-python
clear
echo -n "Did MySQL install prompt you for a root password?[Y/n]"
read yn
if [[ "$yn" == "N" || "$yn" == "n" ]]; then
clear
read -s -p 'Enter the password you want for your MySQL root user' pw
echo ''
sudo mysql <<EOF
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password by '$pw';
flush privileges;
EOF
fi
clear 
echo 'Do you want to install the retail case study?[Y/n]'
read yn2
if [[ "$yn2" == "Y" || "$yn2" == "y" ]]; then
#Downloads the Repo and moves files where they need to be
wget -P ~ https://github.com/RCS-Training-12-18/Retail-Case-Study/tarball/master
tar -xzvf ~/master
rm ~/master
mv ~/RCS-* ~/Retail-Case-Study
mkdir ~/airflow/dags
cp ~/Retail-Case-Study/dags/fm-dag.py ~/airflow/dags
#Replaces some lines in the dag
sed -i 's?msr?'$USER'?' ~/airflow/dags/fm-dag.py
sed -i "/py_file_loc =/c\py_file_loc = '/home/$USER/Retail-Case-Study'" ~/airflow/dags/fm-dag.py
rm -r ~/Retail-Case-Study/dags
rm -r ~/Retail-Case-Study/Script
#Creates the user that part1.py uses
read -s -p 'Enter your MySQL root password' mysqlpw
sudo mysql -u root -p$mysqlpw <<EOS
create user 'user'@'localhost' identified by 'password';
create database foodmart;
grant all privileges on foodmart.* to 'user'@'localhost';
flush privileges;
EOS
#Downloads the foodmart sql data and moves it into the database
wget -P ~ 'http://pentaho.dlpage.phi-integration.com/mondrian/mysql-foodmart-database/foodmart_mysql.tar.gz?attredirects=0&d=1'
tar -xzvf ~/foodmart*
rm ~/foodmart_mysql.t*
mysql -u user -ppassword foodmart < ~/foodmart_my*
#cleanup
rm ~/foodmart*
bash ~/Retail-Case-Study/startup.bash
rm ~/Retail-Case-Study/startup.bash
rm ~/Retail-Case-Study/README.md
fi
clear
#Setup Boto3
echo "Create Boto3 credential files?[Y/n]"
read yn
if [[ "$yn" == "Y" || "$yn" == "y" ]] ; then
mkdir ~/.aws
echo "AWS Username: "
read uname
echo "AWS Secret Key: "
read key
echo "Region: "
read region
clear
echo "[default]" >> ~/.aws/credentials
echo "aws_access_key_id = "$uname >> ~/.aws/credentials
echo "aws_secret_access_key = "$key >> ~/.aws/credentials
echo "[default]" >> ~/.aws/config
echo "region="$region >> ~/.aws/config
#Cycle while boto3 credentials are invalid
python ~/Retail-Case-Study/Script/boto3test.py
RET=$?
while [[ $RET != 0 ]]; do
clear
if [[ $RET == 7 ]] ; then
  echo 'Invalid Access Key. Please re-enter your access key.'
  read uname
  sed -i "/aws_access/c\aws_access_key_id = "$uname"" ~/.aws/credentials
fi
if [[ $RET == 13 ]] ; then
  echo 'Invalid Secret Key. Please re-enter your secret key.'
  read key
  sed -i "/aws_sec/c\aws_secret_access_key = "$key"" ~/.aws/credentials
fi
python ~/Retail-Case-Study/Script/boto3test.py
RET=$?
done
clear
#Set up S3 Bucket
echo "Create an S3 Bucket?[Y/n]"
read yn
if [[ "$yn" == "Y" || "$yn" == "y" ]] ; then
echo "Enter a bucket name. Bucket names must be unique."
read bucket
python ~/Retail-Case-Study/Script/createbucket.py bucket
RET=$?
#Loop while bucket name already exists
if [[ $RET == 3 ]]; then
  echo "Invalid bucket name. Please enter a new bucket name."
  read bucket
  python ~/Retail-Case-Study/Script/createbucket.py $bucket
  RET=$?
fi
sed -i '/bucket_name = /c\bucket_name = "'$bucket'"' Retail-Case-Study/part1.py
sed -i '/bucket_name = /c\bucket_name = "'$bucket'"' Retail-Case-Study/part2.py
sed -i '/bucket_name = /c\bucket_name = "'$bucket'"' Retail-Case-Study/part3.py
sed -i '/bucket_name = /c\bucket_name = "'$bucket'"' Retail-Case-Study/part4.py
fi
fi

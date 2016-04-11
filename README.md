# Idea
Read Me
Setting up and running the build:
Create a third-party directory

>mkdir ~/third-party

KAFKA ():
—————
Use the following URL to find a download site:
http://apache.claz.org/kafka/0.9.0.1/kafka_2.10-0.9.0.1.tgz

curl -0  http://apache.claz.org/kafka/0.9.0.1/kafka_2.10-0.9.0.1.tgz >kafka_2.10-0.9.0.1.tgz

extract the contents (I usually do this in the ~/third-party directory)
tar -xzvf kafka_2.10-0.9.0.1.tgz

ZOOKEEPER (Use the one included in Kafka):
———————— 
Use the following URL to find a mirror in case the link below fails : http://www.apache.org/dyn/closer.cgi/zookeeper/

curl -0 http://mirror.symnds.com/software/Apache/zookeeper/stable/zookeeper-3.4.8.tar.gz >zookeeper-3.4.8.tar.gz

extract the contents (I usually do this in ~/third-party directory)
tar -xzvf zookeeper-3.4.8.tar.gz

add the following line to $KAFKA_HOME/config/zookeeper.properties:
zookeeper.connect=${IP}:2181

CONFIGURE KAFKA AND ZOOKEEPER:
——————————————————
Configure Kafka
————————
Configure the kafka server by editing the config/server.properties file under the kafka directory. Follow the instructions here:
To start the single broker do the following
######server.properties (FOUNT IN <KAFKA-INSTALL-DIR>/config)
# The id of the broker. This must be set to a unique integer for each broker.
broker.id = 1

# The port the socket server listens to 
port=9092

#Hostname the broker will bind to. If not set, the server will bind to all interfaces
#host.name=localhost

# A comma separated list of directories under which to store log files
log.dirs=/tmp/kafka-logs

# The default number of log partitions per topic. More partitions allow greater
# parallelism for consumption, but this will also result in more files across
# the brokers.
num.partitions=1

#modify/add the following settings
zookeeper.session.timeout.ms=10000
# Timeout in ms for connecting to zookeeper
zookeeper.connection.timeout.ms=6000

Configure Zookeeper properties
-------------------------------
Copy the zookeeper properties file from the kafka directory to ~/third-party/zookeeper-3.4.8/conf
by typing the following:

~/third-party/zookeeper-3.4.8:> cp ../kafka_2.10-0.9.0.1/config/zookeeper.properties conf/zoo.cfg

CASSANDRA () :
———————
Use the following URL to download cassandra:
http://www.apache.org/dyn/closer.lua/cassandra/3.4/

curl -0 http://www-eu.apache.org/dist/cassandra/3.4/apache-cassandra-3.4-bin.tar.gz >apache-cassandra-3.4-bin.tar.gz


extract the contents (I usually do this in ~/third-party directory)
tar -xzvf apache-cassandra-3.4-bin.tar.gz

Configure Cassandra
——————————
Open your cassandra.yaml file and do the following: Check if you have the apache thrift rpc server enabled and if it is listening on the port Kairos is listening. 

start_rpc: true 
rpc_address: localhost 
rpc_port: 9160

KAIROSDB () :
——————
Use the following URL to download Kairosdb
https://github.com/kairosdb/kairosdb/releases/download/

curl -0 https://github.com/kairosdb/kairosdb/releases/download/v1.1.1/kairosdb-1.1.1-1.tar.gz >kairosdb-1.1.1-1.tar.gz

extract the contents (I usually do this in ~/third-party directory)
tar -xzvf kairosdb-1.1.1-1.tar.gz

Configure Kairosdb (Enable Cassandra)
——————————————————
Go to /conf/kairosdb.properties file and comment out the line:
#kairosdb.service.datastore=org.kairosdb.datastore.h2.H2Module

And uncomment the line:
kairosdb.service.datastore=org.kairosdb.datastore.cassandra.CassandraModule


===================================================
Starting the third-party servers :
===================================================
Add it in the path as environment variables
vim ~/.bash_profile

Set the following environment variables appropriately:
export ZOOKEEPER_HOME=~/third-party/zookeeper-3.4.8
export KAFKA_HOME=~/third-party/kafka_2.10-0.9.0.1
export CASSANDRA_HOME=~/third-party/apache-cassandra-3.4
export KAIROSDB_HOME=~/third-party/kairosdb

source ~/.bash_profile 


First: -  Start the zookeeper instance
cd $ZOOKEEPER_HOME && ./bin/zkServer.sh start-foreground

Second: - Start the single Kafka server
cd $KAFKA_HOME && bin/kafka-server-start.sh config/server.properties

Third: - Start cassandra
cd $CASSANDRA_HOME 
sudo bin/cassandra -f 
(to run on foreground)
(use Ctrl + C to stop)

OR
sudo bin/cassandra (to run on background)
(ps waux | grep cassandra) - to find all the running services with the name “cassandra"
(pkill -f CassandraDaemon to stop) - didnt work in my case, I used sudo kill -9 

Fourth: - Start kairosdb
(UPDATE: first run sudo ./kairosdb.sh run !!! then you can stop and run in background. Otherwise, you may experience some problems trying to launch it and make it work.)

cd $KAIROSDB_HOME
sudo ./kairosdb.sh run  
 (this will run kairosdb in foreground)
(use Ctrl + C to stop)

OR
> sudo ./kairosdb.sh start (this will run kairosdb in background)
> sudo ./kairosdb.sh stop  (to stop kairosdb)

===========================
Set ups the Application
===========================

 

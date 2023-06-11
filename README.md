
# MapReduce

For this project, we will use MapReduce to find the most profitable month for hotel bookings over
a period of four years. To do this, compute the total revenues recorded every month and arrange
them in descending order. This can give us some interesting information like which months of the
year show higher revenue for a particular market segment.\
The problem is that the hotel company decided to change the way they keep their data, so there
are two separate datasets with different schemas (but similar information) that we need to use.\
\
The first dataset (hotel-bookings) provides booking information from 2015 to 2016. The second
dataset (customer-reservations) provides information about the reservation details from 2017 to 2018. More detailed explanation of the two datasets is below (note: you may not need all the
columns for each dataset).\
\
The goal of the project is to find out which month was most profitable from 2015-2018.


## Hadoop Installation

[Hadoop Installation on Ubuntu](https://phoenixnap.com/kb/install-hadoop-ubuntu)


## STEPS

## 1.	Start Hadoop

Inside Hadoop user Id

```bash
sudo service ssh start
ssh localhost

```
Format the HDFS Namenode

```bash
hdfs namenode -format
```
Start the Hadoop Cluster\
\
Inside Hadoop/sbin

```bash
./start-dfs.sh
./start-yarn.sh
jps

```
Access Hadoop UI from Browser
```bash
http://localhost:9870
```

This command retrieves the classpath required for Hadoop
```bash
export HADOOP_CLASSPATH=$(hadoop classpath)
```
## 2.	Preprocessing Part
In Hadoop Cluster\
\
Create following directories

```bash
hdfs dfs -mkdir /preprocessing
hdfs dfs -mkdir /preprocessing/Input
hdfs dfs -mkdir /preprocessing/Output
hdfs dfs -mkdir /max
hdfs dfs -mkdir /max/Input
hdfs dfs -mkdir /max/Output
hdfs dfs -mkdir /sorting
hdfs dfs -mkdir /sorting/Input
hdfs dfs -mkdir /sorting/Output
```
Adding hotel-booking.csv and customer-reservations.csv inside preprocessing/Input folder.
```bash
hadoop fs -put '/home/hduser_/DBMSPROJECT/hotel-booking.csv' /preprocessing/Input
hadoop fs -put '/home/hduser_/DBMSPROJECT/customer-reservations.csv'  /preprocessing/Input

```

Verify the Files
```bash
hadoop fs -ls /preprocessing/Input
```
Not on Hadoop Cluster Only in Hadoop local\
\
Copy and Paste the Combiner.java\
Compile the Combiner.java file using javac, specifying the classpath and output directory.

```bash
sudo javac -classpath  ${HADOOP_CLASSPATH} -d '/home/hduser_/DBMSPROJECT/classes' '/home/hduser_/DBMSPROJECT/Combiner.java'
```
Create a JAR file named combine.jar from the contents of the classes/ directory.
```bash
sudo jar -cvf combine.jar -C classes/ .
```
Run a Hadoop job using the combine.jar JAR file, specifying the main class and input/output paths.
```bash
hadoop jar '/home/hduser_/DBMSPROJECT/combine.jar' Combiner /preprocessing/Input  /preprocessing/Output
```
In Hadoop Cluster \
\
Verify the output file for preprocessing
```bash
hadoop fs -ls / preprocessing /Output
```
This command Hadoop is used to display the contents of a file stored in HDFS
```bash
hadoop fs -cat /preprocessing/Output/part-r-00000
```
Copy the output of the Combiner to the Input of Max
```bash
hadoop fs -cp /preprocessing/Output/part-r-00000 /max/Input
```
Verify the file
```bash
hadoop fs -ls /max /Input
```
## 3.	Max Part
Not on Hadoop Cluster Only in Hadoop local\
\
Copy and Paste the Max.java\
Compile the Max.java file using javac, specifying the classpath and output directory.

```bash
sudo javac -classpath  ${HADOOP_CLASSPATH} -d '/home/hduser_/DBMSPROJECT/classes' '/home/hduser_/DBMSPROJECT/Max.java'
```
Create a JAR file named max.jar from the contents of the classes/ directory.
```bash
sudo jar -cvf max.jar -C classes/ .
```
Run a Hadoop job using the max.jar JAR file, specifying the main class and input/output paths.
```bash
hadoop jar '/home/hduser_/DBMSPROJECT/max.jar' Max /max/Input  /max/Output
```

In Hadoop Cluster\
\
Verify the output file 

```bash
hadoop fs -ls /max /Output
```
This command Hadoop is used to display the contents of a file stored in HDFS
```bash
hadoop fs -cat /max/Output/part-r-00000
```

Copy the output of the Max to the Input of Sorting
```bash
hadoop fs -cp /max/Output/part-r-00000 /sorting/Input
```
Verify the file
```bash
hadoop fs -ls /sorting /Input

```
## 4.	Sorting Part
Not on Hadoop Cluster Only in Hadoop local\
\
Copy and Paste the Sorting.java\
Compile the Sorting.java file using javac, specifying the classpath and output directory.

```bash
sudo javac -classpath  ${HADOOP_CLASSPATH} -d '/home/hduser_/DBMSPROJECT/classes' '/home/hduser_/DBMSPROJECT/Sorting.java'
```
Create a JAR file named sorting.jar from the contents of the classes/ directory.
```bash
sudo jar -cvf sorting.jar -C classes/ .
```
Run a Hadoop job using the sorting.jar JAR file, specifying the main class and input/output paths.
```bash
hadoop jar '/home/hduser_/DBMSPROJECT/sorting.jar' Sorting  /sorting/Input/part-r-00000  /sorting/Output
```
In Hadoop Cluster\
\
Verify the output 


```bash
hadoop fs -ls /sorting /Output
```
This command Hadoop is used to display the contents of a file stored in HDFS
```bash
hadoop fs -cat /sorting/Output/part-r-00000
```
## 5.	Stopping Hadoop
Inside Hadoop/sbin
```bash
./stop-dfs.sh
./stop-yarn.sh
```




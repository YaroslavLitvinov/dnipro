The client do copying of all tables from specified postgres database to folder on hdfs.

Invocation example:
python client-sqoop-db-copy.py -t postgresql --sqoop2-host host --dbport 5432 --dbhost host2 --dbhost-job host3 -u username -db somedbname -s public -dd org.postgresql.Driver -from 6 -to 5 --hdfs-output-path /path-to-hdfs-dir --concurrent-jobs-count 4

Prerequisites: 
sqoop2, python-psycopg2


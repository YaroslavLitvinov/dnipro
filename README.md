The client is copying all tables from specified database into folder on hdfs.
It's only expects for existing sqoop2 link which related to 'TO' connection.
Another sqoop2 stuff as links and jobs will be created automatically if they are not located.
Jobs will be automatically executed.
Here was used python API DB, and it's easy to add support for new databases. Currently only postgresql is supported.

Some notes:
In most cases only one --dbhost option is needed, but two dbhost options can be provided for case when port forward is using and dbhosts are different for cluster machines and for computer where script is executing. 

Invocation example:
python client-sqoop-db-copy.py -t postgresql --sqoop2-host host --dbport 5432 --dbhost host2 --dbhost-job host3 -u username -db somedbname -s public -dd org.postgresql.Driver -from 6 -to 5 --hdfs-output-path /path-to-hdfs-dir --concurrent-jobs-count 4

Prerequisites: 
sqoop2, python-psycopg2


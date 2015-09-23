import sys
import time
import getpass
import json
import httplib
import urllib
import argparse
import copy
import dbproxy

# This client is using predefined keys to get map values as
# a map is used as data structure for saving various data
CONN_ID = "connector id"
TO_CONN_ID = "to connector id"
FROM_CONN_ID = "from connector id"
LINK_ID = "link id"
TO_LINK_ID = "to link id"
FROM_LINK_ID = "from link id"
DB_TYPE_NAME = "db type name"
DB_PORT = "db port"
DB_HOST_NAME = "db host name"
DB_DRIVER_NAME = "db driver"
DB_NAME = "db name"
DB_USER_PASS = "db user pass"
TABLE_NAME = "table name"
SCHEMA_NAME = "schema name"
DB_USER_NAME = "db user name"
JOB_ID = "job id"
HDFS_PATH = "hdfs path"
PRIMARY_KEY = "primary key"


def issue_put_get_request(conn, reqtype, request):
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "User-Agent": "Dnipro/0.1",
               "Accept": "application/json"}
    conn.request(reqtype, request, headers=headers)
    return json.loads(conn.getresponse().read())


def issue_post_request(conn, request, body):
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "User-Agent": "Dnipro/0.1",
               "Accept": "application/json"}
    conn.request('POST', request, body, headers=headers)
    return json.loads(conn.getresponse().read())


def get_jobs(sqoop_conn):
    return issue_put_get_request(sqoop_conn, 'GET',
                                 '/sqoop/v1/job/all?user.name=xxx')


def get_links(sqoop_conn):
    return issue_put_get_request(sqoop_conn, 'GET',
                                 '/sqoop/v1/link/all?user.name=xxx')


def start_job(sqoop_conn, jid):
    return issue_put_get_request(sqoop_conn, 'PUT',
                                 '/sqoop/v1/job/%d/start?user.name=xxx' % jid)


def get_job_status(sqoop_conn, jid):
    return issue_put_get_request(sqoop_conn, 'GET',
                                 '/sqoop/v1/job/%d/status?user.name=xxx' % jid)


def create_sqoop_link(sqoop_conn, connector):
    ts = int(time.time())
    conn_str = "jdbc:%s://%s:%d/%s" % (connector[DB_TYPE_NAME],
                                       connector[DB_HOST_NAME],
                                       connector[DB_PORT],
                                       connector[DB_NAME])
    body = json.dumps(
        {
            "link": {
                "creation-user": None,
                "name": "%s_%s" % (connector[DB_TYPE_NAME][:3],
                                   connector[DB_NAME]),
                "update-user": None,
                "link-config-values": [{
                    "inputs": [{
                        "sensitive": False,
                        "name": "linkConfig.jdbcDriver",
                        "editable": "ANY",
                        "overrides": "",
                        "type": "STRING",
                        "id": 31,
                        "value": connector[DB_DRIVER_NAME],
                        "size": 128
                    }, {
                        "sensitive": False,
                        "name": "linkConfig.connectionString",
                        "editable": "ANY",
                        "overrides": "",
                        "type": "STRING",
                        "id": 32,
                        "value": urllib.quote(conn_str, safe=''),
                        "size": 128
                    }, {
                        "sensitive": False,
                        "name": "linkConfig.username",
                        "editable": "ANY",
                        "overrides": "",
                        "type": "STRING",
                        "id": 33,
                        "value": urllib.quote(connector[DB_USER_NAME],
                                              safe=''),
                        "size": 40
                    }, {
                        "sensitive": True,
                        "name": "linkConfig.password",
                        "editable": "ANY",
                        "overrides": "",
                        "type": "STRING",
                        "id": 34,
                        "value": urllib.quote(connector[DB_USER_PASS],
                                              safe=''),
                        "size": 40
                    }, {
                        "name": "linkConfig.jdbcProperties",
                        "overrides": "",
                        "editable": "ANY",
                        "value": {
                            "protocol": "tcp"
                        },
                        "sensitive": False,
                        "type": "MAP",
                        "id": 35
                    }],
                    "type": "LINK",
                    "id": 13,
                    "name": "linkConfig"
                }, {
                    "inputs": [{
                        "name": "dialect.identifierEnclose",
                        "overrides": "",
                        "editable": "ANY",
                        "sensitive": False,
                        "type": "STRING",
                        "id": 36,
                        "size": 5
                    }],
                    "type": "LINK",
                    "id": 14,
                    "name": "dialect"
                }],
                "connector-id": connector[CONN_ID],
                "update-date": ts,
                "enabled": True,
                "id": -1,
                "creation-date": ts
            }
        }
    )
    print issue_post_request(sqoop_conn, '/sqoop/v1/link/?user.name=xxx', body)


def create_sqoop_job(sqoop_conn, copy_from, copy_to):
    ts = int(time.time())
    hdfs_path = copy_to[HDFS_PATH] + "/" + copy_to[TABLE_NAME]
    body = json.dumps(
        {
            "job": {
                "driver-config-values": [{
                    "id": 18,
                    "inputs": [{
                        "id": 51,
                        "overrides": "",
                        "name": "throttlingConfig.numExtractors",
                        "value": "1",
                        "type": "INTEGER",
                        "editable": "ANY",
                        "sensitive": False
                    }, {
                        "id": 52,
                        "overrides": "",
                        "name": "throttlingConfig.numLoaders",
                        "type": "INTEGER",
                        "editable": "ANY",
                        "sensitive": False
                    }],
                    "name": "throttlingConfig",
                    "type": "JOB"
                }],
                "enabled": True,
                "from-connector-id": copy_from[CONN_ID],
                "update-user": None,
                "to-config-values": [{
                    "id": 12,
                    "inputs": [{
                        "id": 24,
                        "overrides": "",
                        "name": "toJobConfig.overrideNullValue",
                        "type": "BOOLEAN",
                        "editable": "ANY",
                        "sensitive": False
                    }, {
                        "id": 25,
                        "overrides": "",
                        "name": "toJobConfig.nullValue",
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 255,
                        "sensitive": False
                    }, {
                        "id": 26,
                        "values": "TEXT_FILE,SEQUENCE_FILE",
                        "overrides": "",
                        "name": "toJobConfig.outputFormat",
                        "value": "TEXT_FILE",
                        "type": "ENUM",
                        "editable": "ANY",
                        "sensitive": False
                    }, {
                        "id": 27,
                        "values": "NONE,DEFAULT,DEFLATE,"
                        "GZIP,BZIP2,LZO,LZ4,SNAPPY,CUSTOM",
                        "overrides": "",
                        "name": "toJobConfig.compression",
                        "value": "NONE",
                        "type": "ENUM",
                        "editable": "ANY",
                        "sensitive": False
                    }, {
                        "id": 28,
                        "overrides": "",
                        "name": "toJobConfig.customCompression",
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 255,
                        "sensitive": False
                    }, {
                        "id": 29,
                        "overrides": "",
                        "name": "toJobConfig.outputDirectory",
                        "value": urllib.quote(hdfs_path, safe=''),
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 255,
                        "sensitive": False
                    }, {
                        "id": 30,
                        "overrides": "",
                        "name": "toJobConfig.appendMode",
                        "type": "BOOLEAN",
                        "editable": "ANY",
                        "sensitive": False
                    }],
                    "name": "toJobConfig",
                    "type": "JOB"
                }],
                "to-connector-id": copy_to[CONN_ID],
                "creation-date": ts,
                "update-date": ts,
                "creation-user": None,
                "id": -1,
                "to-link-id": 1,
                "from-config-values": [{
                    "id": 15,
                    "inputs": [{
                        "id": 37,
                        "overrides": "",
                        "name": "fromJobConfig.schemaName",
                        "value": urllib.quote(copy_from[SCHEMA_NAME], safe=''),
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 50,
                        "sensitive": False
                    }, {
                        "id": 38,
                        "overrides": "",
                        "name": "fromJobConfig.tableName",
                        "value": urllib.quote(copy_from[TABLE_NAME], safe=''),
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 50,
                        "sensitive": False
                    }, {
                        "id": 39,
                        "overrides": "",
                        "name": "fromJobConfig.sql",
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 2000,
                        "sensitive": False
                    }, {
                        "id": 40,
                        "overrides": "",
                        "name": "fromJobConfig.columns",
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 50,
                        "sensitive": False
                    }, {
                        "id": 41,
                        "overrides": "",
                        "name": "fromJobConfig.partitionColumn",
                        "value": urllib.quote(copy_from[PRIMARY_KEY], safe=''),
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 50,
                        "sensitive": False
                    }, {
                        "id": 42,
                        "overrides": "",
                        "name":
                        "fromJobConfig.allowNullValueInPartitionColumn",
                        "type": "BOOLEAN",
                        "editable": "ANY",
                        "sensitive": False
                    }, {
                        "id": 43,
                        "overrides": "",
                        "name": "fromJobConfig.boundaryQuery",
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 50,
                        "sensitive": False
                    }],
                    "name": "fromJobConfig",
                    "type": "JOB"
                }, {
                    "id": 16,
                    "inputs": [{
                        "id": 44,
                        "overrides": "",
                        "name": "incrementalRead.checkColumn",
                        "type": "STRING",
                        "editable": "ANY",
                        "size": 50,
                        "sensitive": False
                    }, {
                        "id": 45,
                        "overrides": "",
                        "name": "incrementalRead.lastValue",
                        "type": "STRING",
                        "editable": "ANY",
                        "size": -1,
                        "sensitive": False
                    }],
                    "name": "incrementalRead",
                    "type": "JOB"
                }],
                "name": urllib.quote("%s_%s_%s" % (copy_from[DB_TYPE_NAME][:3],
                                                   copy_from[DB_NAME][:3],
                                                   copy_from[TABLE_NAME]),
                                     safe=''),
                "from-link-id": copy_from[LINK_ID]
            }
        }
    )
    return issue_post_request(sqoop_conn, '/sqoop/v1/job/?user.name=xxx', body)


def get_links_as_list_of_maps(js_links):
    l = []
    for item in js_links["links"]:
        m = {CONN_ID: item["connector-id"]}
        m[LINK_ID] = item["id"]
        for input_ in item["link-config-values"][0]["inputs"]:
            if input_["name"] == "linkConfig.jdbcDriver":
                m[DB_DRIVER_NAME] = input_["value"]
            elif input_["name"] == "linkConfig.connectionString":
                value = urllib.unquote(input_["value"])
                m[DB_TYPE_NAME] = value.split(':')[1]
                m[DB_NAME] = value.split('/')[-1]
        l.append(m)
    return l


def get_jobs_as_list_of_maps(js_jobs):
    l = []
    for item in js_jobs["jobs"]:
        m = {JOB_ID: int(item["id"]),
             TO_LINK_ID: int(item["to-link-id"]),
             FROM_LINK_ID: int(item["from-link-id"]),
             TO_CONN_ID: int(item["to-connector-id"]),
             FROM_CONN_ID: int(item["from-connector-id"]),
             TABLE_NAME: item["name"]}
        l.append(m)
    return l


def get_submission_as_map(js_submission):
    key = "submission"
    if key in js_submission:
        return js_submission[key]
    else:
        return None


def get_job_creation_err_mes(js_error, table_name):
    return "job %s creation failed. %s %s " % (table_name,
                                               js_error['error-code'],
                                               js_error['message'])


def get_submission_err_mes(js_error, jobid, table_name):
    p = copy.copy(js_error)
    del p["stack-trace"]
    return "job %d / %s submission failed.\n%s" % (jobid,
                                                   table_name, str(p))


def get_submission_status_mes(js_status, table_name):
    last_update_date = int(js_status["last-update-date"])
    creation_date = int(js_status["creation-date"])
    micro_seconds = last_update_date - creation_date
    fs_counter_key = "org.apache.hadoop.mapreduce.FileSystemCounter"
    bytes_written = js_status["counters"][fs_counter_key]["HDFS_BYTES_WRITTEN"]
    return "job %d / %s status %s. time elapsed: %d seconds, \
bytes written: %s" % (js_status["job-id"],
                      table_name, js_status["status"],
                      int(micro_seconds/1000), str(bytes_written))


def handle_db_table_create_job(sqoop_conn, table_name,
                               partit_column_name, t, f):
    to_hdfs_copy = copy.copy(t)
    from_db_copy = copy.copy(f)

    to_hdfs_copy[TABLE_NAME] = table_name  # to be used as subpath
    from_db_copy[TABLE_NAME] = table_name  # to be used as sql table name
    from_db_copy[PRIMARY_KEY] = partit_column_name
    return create_sqoop_job(sqoop_conn, from_db_copy, to_hdfs_copy)


def find_table_in_sqoop_jobs_list(jobs, table_name):
    for item in jobs:
        if TABLE_NAME in item and item[TABLE_NAME] == table_name:
            return True
    return False


def get_single_match_by(maps_array, conn_data, match_list):
    for item in maps_array:
        if is_match_by(item, conn_data, match_list) is not None:
            return item
    return None


def get_array_matches_by(maps_array, conn_data, match_list):
    l = []
    for item in maps_array:
        if is_match_by(item, conn_data, match_list) is not None:
            l.append(item)
    return l


def map_token_matcher(key, match_list, item, conn_data, cc):
    if key not in match_list:
        return
    if key in conn_data:
        cc[0] += 1
    if key in item and \
       key in conn_data and \
       item[key] == conn_data[key]:
        cc[1] += 1


def is_match_by(item, conn_data, match_list):
    # use `cc` as mutable container
    cc = [0, 0]
    map_token_matcher(CONN_ID, match_list, item, conn_data, cc)
    map_token_matcher(TO_CONN_ID, match_list, item, conn_data, cc)
    map_token_matcher(FROM_CONN_ID, match_list, item, conn_data, cc)
    map_token_matcher(DB_NAME, match_list, item, conn_data, cc)
    map_token_matcher(DB_TYPE_NAME, match_list, item, conn_data, cc)
    map_token_matcher(LINK_ID, match_list, item, conn_data, cc)
    map_token_matcher(TO_LINK_ID, match_list, item, conn_data, cc)
    map_token_matcher(FROM_LINK_ID, match_list, item, conn_data, cc)
    map_token_matcher(TABLE_NAME, match_list, item, conn_data, cc)

    if cc[0] == cc[1] and cc[0] != 0:
        return item
    else:
        return None

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--dbtype", help="database type",
                        type=str, required=True)
    parser.add_argument("--sqoop2-host", help="sqoop server host",
                        type=str, required=True)
    parser.add_argument("--dbport", help="database port",
                        type=int, required=True)
    parser.add_argument("--dbhost", help="database host",
                        type=str, required=True)
    parser.add_argument("--dbhost-job",
                        help="database host can be used by job",
                        type=str, required=False)
    parser.add_argument("-u", "--dbuser",
                        help="database user", type=str, required=True)
    parser.add_argument("-db", "--dbname",
                        help="database name", type=str, required=True)
    parser.add_argument("-s", "--dbschema",
                        help="schema name", type=str, required=True)
    parser.add_argument("-p", "--dbpass", help="db password", type=str)
    parser.add_argument("-dd", "--dbdriver", help="db driver",
                        type=str, required=True)
    parser.add_argument("-from", "--connector-from",
                        help="sqoop2 source connector",
                        type=int, required=True)
    parser.add_argument("-to", "--connector-to",
                        help="sqoop2 target connector",
                        type=int, required=True)
    parser.add_argument("--hdfs-output-path",
                        help="hdfs folder to save results",
                        type=str, required=True)
    parser.add_argument("--concurrent-jobs-count",
                        help="How many jobs will be executed concurrently",
                        type=int, required=True)
    args = parser.parse_args()

    if not args.dbpass:
        args.dbpass = getpass.getpass("Enter db password for \
user %s :\n" % args.dbuser)

    if not args.dbhost_job:
        args.dbhost_job = args.dbhost

    db_proxy = dbproxy.create(args.dbtype, args.dbhost, args.dbport,
                              args.dbname, args.dbuser, args.dbpass,
                              args.dbschema)
    db_proxy.open()

    to_hdfs = {CONN_ID: args.connector_to,
               TO_CONN_ID: args.connector_to,
               HDFS_PATH: args.hdfs_output_path}
    from_db = {CONN_ID: args.connector_from,
               FROM_CONN_ID: args.connector_from,
               DB_TYPE_NAME: args.dbtype,
               DB_HOST_NAME: args.dbhost_job,
               DB_PORT: args.dbport,
               DB_USER_NAME: args.dbuser,
               DB_USER_PASS: args.dbpass,
               DB_DRIVER_NAME: args.dbdriver,
               DB_NAME: args.dbname,
               SCHEMA_NAME: args.dbschema}

    sqoop_conn = httplib.HTTPConnection(args.sqoop2_host, 12000)
    sqoop_conn.connect()

    # deal with links
    links = get_links_as_list_of_maps(get_links(sqoop_conn))
    item = get_single_match_by(links, to_hdfs, [CONN_ID])
    if item:
        to_hdfs[LINK_ID] = item[LINK_ID]
        to_hdfs[TO_LINK_ID] = item[LINK_ID]
    else:
        sys.exit("Can't found a link corresponding to 'TO' connector")

    # ensure link exist
    item = get_single_match_by(links, from_db,
                               [CONN_ID, DB_NAME, DB_TYPE_NAME])
    if item is None:
        create_sqoop_link(sqoop_conn, from_db)
        links_map = get_links_as_list_of_maps(get_links(sqoop_conn))
        item = get_single_match_by(links_map,
                                   from_db, [CONN_ID, DB_NAME, DB_TYPE_NAME])
        if item is None:
            sys.exit("Can't create a link corresponding to 'FROM' connector")

    from_db[FROM_LINK_ID] = from_db[LINK_ID] = item[LINK_ID]
    tables = db_proxy.tables()

    # get only jobs related to our database
    jobs = get_array_matches_by(
        get_jobs_as_list_of_maps(get_jobs(sqoop_conn)),
        from_db, [FROM_LINK_ID])
    # ensure that we have all jobs created
    for table_name in tables:
        if not find_table_in_sqoop_jobs_list(jobs, table_name):
            print "add job", table_name
            pk = db_proxy.primary_key_of_table(table_name)
            if pk is None:
                columns = db_proxy.columns_of_table(table_name)
                if columns:
                    pk = columns[0]
            # create job if table not in sqoop jobs list
            js_res = handle_db_table_create_job(sqoop_conn, table_name,
                                                pk, to_hdfs, from_db)
            if 'error-code' in js_res:
                print get_job_creation_err_mes(js_res, table_name)
            else:
                print js_res
    db_proxy.close()
    # get only jobs related to specified database
    jobs = get_array_matches_by(
        get_jobs_as_list_of_maps(get_jobs(sqoop_conn)),
        from_db, [FROM_LINK_ID])
    running_jobs = []
    jobs_statuses = {}
    # submit all jobs
    while jobs or running_jobs:
        # check statuses of submitted jobs and remove failed, completed ones
        for index in reversed(xrange(len(running_jobs))):
            rjob = running_jobs[index]
            job_status = get_job_status(sqoop_conn, rjob[JOB_ID])
            submission = get_submission_as_map(job_status)
            if submission["status"] == "SUCCEEDED" or \
               submission["status"] == "FAILED":
                jobs_statuses[rjob[JOB_ID]] = submission
                running_jobs.pop(index)
                print get_submission_status_mes(submission, rjob[TABLE_NAME])

        # submit
        while jobs and len(running_jobs) < args.concurrent_jobs_count:
            job_to_start = jobs.pop()
            job_status = start_job(sqoop_conn, job_to_start[JOB_ID])
            jobs_statuses[job_to_start[JOB_ID]] = job_status
            submission = get_submission_as_map(job_status)
            if submission is None:
                error = job_status
                print get_submission_err_mes(error,
                                             job_to_start[JOB_ID],
                                             job_to_start[TABLE_NAME])
            else:
                running_jobs.append(job_to_start)
        if len(running_jobs) > 0:
            time.sleep(10)
    print "finish"

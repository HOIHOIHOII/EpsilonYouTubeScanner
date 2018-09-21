from harvest_data import *

#this test script is only for running against the remote epsilo db

dbname = "epsilo"
user = "postgres"
password = "password"

conn, cur = make_db_connection(dbname, user, password)

update_videos(conn, cur, 10)
# NOTE I HAVE NOT TRIED IT YET BUT I THINK IT CAN HELP
# IF YOU HAVE SOME ERROR THAT ASKS YOU TO GO TO stl_load_errors FOR MORE DETAILS THIS SCRIPT CAN HELP YOU CATCH THEM
import configparser
import psycopg2
config = configparser.ConfigParser()
config.read('dwh.cfg')
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()
qry = "SELECT * FROM stl_load_errors errors"
cur.execute(qry)
OUTPUT = cur.fetchall()
print(OUTPUT)
conn.close()

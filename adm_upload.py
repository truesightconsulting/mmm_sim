import sys
#client_path=sys.argv[1]
#main_path=sys.argv[2]
#user_id=int(sys.argv[3])
#client_id=int(sys.argv[4])
#db_server=sys.argv[5]
#db_name=sys.argv[6]
#port=int(sys.argv[7])
#username=sys.argv[8]
#password=sys.argv[9]
#file_name=sys.argv[10]
#save_name=sys.argv[11]

client_path='c:/Users/XinZhou/Documents/GitHub/demo/'
main_path='c:/Users/XinZhou/Documents/GitHub/mmm_sim/'
user_id=1
client_id=31
# DB server info
is_staging=True
db_server="bitnami.cluster-chdidqfrg8na.us-east-1.rds.amazonaws.com"
db_server="127.0.0.1"
db_name="nviz"
port=3306
if is_staging:
    username="root"
    password="bitnami"
else:
    username="Zkdz408R6hll"
    password="XH3RoKdopf12L4BJbqXTtD2yESgwL$fGd(juW)ed"
file_name='c:/Users/XinZhou/Documents/GitHub/demo/admin/mmm/upload_cps.csv'
save_name='test1'

import sqlalchemy
import numpy as np
import pandas as pd
import gc
import os
import itertools
import datetime
import functools
import parser
import importlib

# db connection
engine=sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(username,password,db_server,db_name))
conn=engine.connect()

# several checks
save_table=pd.read_sql('select * from mmm_cps where client_id={}'.format(client_id),conn)
if save_name in save_table.name:
    print('Error: The name already exists. Please select another name.')
elif raw_cps.isnull().values.any():
    print("Error: Missing value is not allowed. Please check your file")
elif raw_cps.duplicated('bdgt_id').any():
    print("Error: There is dimension duplication in your file. Please check.")
else:
    raw_cps=pd.read_csv(file_name)
    keep=raw_cps.columns[~raw_cps.columns.str.contains('_name')]
    raw_cps=raw_cps[keep]
    temp=pd.DataFrame({'client_id':client_id,'user_id':user_id,'name':save_name,'created':datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')},index=[0])
    temp.to_sql('mmm_cps',conn,index=False,if_exists='append',flavor='mysql')
    cps_id=pd.read_sql('select id from mmm_cps where name=\'{}\''.format(save_name),conn).id[0]
    raw_cps['cps_id']=cps_id
    raw_cps.to_sql('mmm_cps_save',conn,index=False,if_exists='append',flavor='mysql')
    print("Success")
    print('"Return":{"id":%d}' % (cps_id))
conn.close()
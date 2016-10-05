import sys
#client_path=sys.argv[1]
#main_path=sys.argv[2]
#client_id=int(sys.argv[3])
#db_server=sys.argv[4]
#db_name=sys.argv[5]
#port=int(sys.argv[6])
#username=sys.argv[7]
#password=sys.argv[8]
#mc_server=sys.argv[9]
#mc_port=int(sys.argv[10])
#input_data=sys.argv[11]

client_path='c:/Users/XinZhou/Documents/GitHub/demo/'
main_path='c:/Users/XinZhou/Documents/GitHub/mmm_sim/'
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
# MC server info
mc_server="nviz.mgbx5x.cfg.use1.cache.amazonaws.com"
mc_server="127.0.0.1"
mc_port=11211

import memcache
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

# improt functions module
sys.path.append(main_path)
import mmm_modelinput_functions as mmm
#importlib.reload(mmm)
client_path='{}admin/mmm/'.format(client_path)
os.chdir(client_path)

# fetch data
data=pd.read_csv('plan1.csv')
dim_bdgt=mmm.get_dim_bdgt(client_path)
data=data[dim_bdgt+['bdgt_id','date','value']]
# converting
data.date=pd.to_datetime(data.date,format='%Y-%m-%d').dt.strftime('%m/%d/%Y')
data_pivot=pd.pivot_table(data,index=dim_bdgt+['bdgt_id'],values='value',columns='date',fill_value=0)
data_pivot.reset_index(inplace=True)
engine=sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(username,password,db_server,db_name))
conn=engine.connect()
dim=[i.replace('_id','') for i in dim_bdgt]
for i in dim:
    #i=dim[0]
    tb_name='mmm_label_{}'.format(i)
    temp=pd.read_sql('select * from {}'.format(tb_name),conn)
    temp_dim=pd.read_csv('adm_modules_dim.csv',index_col='dim')
    temp=temp.rename(columns={'id':'{}_id'.format(i),'label':temp_dim.ix['{}_id'.format(i)].label})
    data_pivot=pd.merge(data_pivot,temp,on='{}_id'.format(i),how='left')
conn.close()
# output

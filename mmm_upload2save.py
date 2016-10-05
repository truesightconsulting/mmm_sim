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
input_data='a_plan_wRJB2BIwAcOu8pAmtXUkQVujTkIsP2t1PVdKsadM_31'

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
data=pd.read_csv('plan.csv')
#mc=memcache.Client(["{}:{}".format(mc_server,mc_port)],debug=0)
#data=mc.get(input_data)
# converting
dim_bdgt=mmm.get_dim_bdgt(client_path)
dim_keep=dim_bdgt+['bdgt_id']+data.columns[data.columns.str.contains('/')].tolist()
data=data[dim_keep]
data_melt=pd.melt(data,id_vars=dim_bdgt+['bdgt_id'],var_name='date')
data_melt.date=pd.to_datetime(data_melt.date,format='%m/%d/%Y')
data_melt.to_csv('plan1.csv',index=False)
# output

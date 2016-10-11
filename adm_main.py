def adm(client_path,main_path,is_staging,db_server,db_name,port,username,password):
    #client_path='c:/Users/XinZhou/Documents/GitHub/demo/admin/mmm/'
    #main_path='c:/Users/XinZhou/Documents/GitHub/mmm_sim/'
    ## DB server info
    #is_staging=True
    #db_server="bitnami.cluster-chdidqfrg8na.us-east-1.rds.amazonaws.com"
    #db_server="127.0.0.1"
    #db_name="nviz"
    #port=3306
    #if is_staging:
    #  username="root"
    #  password="bitnami"
    #else:
    #  username="Zkdz408R6hll"
    #  password="XH3RoKdopf12L4BJbqXTtD2yESgwL$fGd(juW)ed"
    
    import sqlalchemy
    import numpy as np
    import pandas as pd
    import gc
    import os
    import itertools
    import datetime
    import functools
    import parser
    import sys
    import importlib
    
    # set pwd
    os.chdir(client_path)
    # improt functions module
    sys.path.append(main_path)
    import mmm_modelinput_functions as mmm
    
    # load data
    adm_setup=pd.read_csv('adm_setup.csv')
    adm_data_var=pd.read_csv('adm_data_var.csv')
    adm_modules=pd.read_csv('adm_modules.csv')
    adm_modules_dim=pd.read_csv('adm_modules_dim.csv')
    adm_output=pd.read_csv('adm_output.csv')
    
    # db connection
    engine=sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(username,password,db_server,db_name))
    conn=engine.connect()
    
    # create client id
    client_name=adm_setup.ix[adm_setup.attribute=='client_name','value'].to_string(index=False)
    r_path=adm_setup.ix[adm_setup.attribute=='r_path','value'].to_string(index=False)
    if adm_setup.ix[adm_setup.attribute=='update','value'].values=='y':
        print('Note: Update client id')
        client_id=pd.read_sql('select id from clients where name=\'{}\''.format(client_name),conn).id.values[0]
        pd.io.sql.execute('update clients set r_script_path=\'{}\' where id={}'.format(r_path,client_id),conn)
    elif adm_setup.ix[adm_setup.attribute=='update','value'].values=='n':
        temp=pd.DataFrame({'name':client_name,'r_script_path':r_path},index=[0])
        if client_name in pd.read_sql('select name from clients',conn).name.values:
            sys.exit("Error: Client name already exists. Please change it.")
        else:
            print('Note: Create client id')
            temp.to_sql('clients',conn,index=False,if_exists='append',flavor='mysql')
            client_id=pd.read_sql('select id from clients where name=\'{}\''.format(client_name),conn).id.values[0]
    
    # update label tables
    print('Note: Update label tables')
    dim=adm_data_var.columns[adm_data_var.columns.str.contains('_name')].tolist()
    for i in dim:
        #i=dim[0]
        temp=pd.DataFrame({'label':adm_data_var[i].unique()})
        name=i.split('_name')[0]
        tb_name='mmm_label_'+name
        label=pd.read_sql('select label from {}'.format(tb_name),conn).label.values
        temp=temp[~temp.label.isin(label)]
        if temp.shape[0]!=0:
            print('Note: New labels inserted in {}'.format(tb_name))
            temp.to_sql(tb_name,conn,index=False,if_exists='append',flavor='mysql')
    
    # create modelinput_var
    print('Note: Create modelinput_var table')
    dim=adm_data_var.columns[adm_data_var.columns.str.contains('_name')].tolist()
    for i in dim:
        #i=dim[0]
        name=i.split('_name')[0]
        tb_name='mmm_label_'+name
        temp=pd.read_sql('select * from {}'.format(tb_name),conn)
        temp=temp.rename(columns={'id':i.replace('_name','_id'),'label':i})
        adm_data_var=pd.merge(adm_data_var,temp,on=i,how='left')
    # add client id
    adm_data_var['client_id']=client_id
    # create bdgt_id
    adm_data_var['bdgt_id']=adm_data_var[mmm.get_dim_bdgt(client_path)].apply(lambda x:'_'.join(x.astype('U')),axis=1)
    # drop _name columns
    adm_data_var=adm_data_var.drop(dim,axis=1)
    # upload to db
    pd.io.sql.execute('delete from mmm_modelinput_var where client_id={}'.format(client_id),conn)
    adm_data_var.to_sql('mmm_modelinput_var',conn,index=False,if_exists='append',flavor='mysql')
    
    # Create setup table
    print("Note: Creating Setup Table")
    date_start=pd.to_datetime(adm_setup.ix[adm_setup.attribute=='time','value'],format='%m/%d/%Y').dt.strftime('%Y-%m-%d').to_string(index=False)
    temp=pd.DataFrame({'client_id':client_id,'date_min':date_start},index=[0])
    pd.io.sql.execute('delete from mmm_input_setup where client_id={}'.format(client_id),conn)
    temp.to_sql('mmm_input_setup',conn,index=False,if_exists='append',flavor='mysql')
    
    # dim selection file
    print("Note: Creating Dimension Tables")
    dim=adm_setup.attribute[adm_setup.attribute.str.contains('dim_')]
    for i in dim:
        #i=dim[5]
        print('Note: Processing input_{}'.format(i))
        dim_temp=adm_setup.ix[adm_setup.attribute==i,'value'].str.split(',').tolist()[0]
        temp=adm_data_var.drop_duplicates(subset=dim_temp)[dim_temp]
        temp['client_id']=client_id
        pd.io.sql.execute('delete from mmm_input_{} where client_id={}'.format(i,client_id),conn)
        temp.to_sql('mmm_input_{}'.format(i),conn,index=False,if_exists='append',flavor='mysql')
    
    # cps table
    print('Note: Creating CPS table')
    temp=adm_data_var[mmm.get_dim_bdgt(client_path)+['bdgt_id','cpp','client_id']].drop_duplicates('bdgt_id').rename(columns={'cpp':'cps'})   
    pd.io.sql.execute('delete from mmm_input_cps where client_id={}'.format(client_id),conn)
    temp.to_sql('mmm_input_cps',conn,index=False,if_exists='append',flavor='mysql')
    dim=mmm.get_dim_bdgt(client_path)
    temp_adm_data_var=adm_data_var.copy()
    for i in dim:
        #i=dim[0]
        tb_name='mmm_label_{}'.format(i.split('_id')[0])
        temp=pd.read_sql('select * from {}'.format(tb_name),conn)
        temp=temp.rename(columns={'id':i,'label':i.replace('_id','_name')})
        temp_adm_data_var=pd.merge(temp_adm_data_var,temp,on=i,how='left') 
    temp=temp_adm_data_var.drop_duplicates('bdgt_id')[dim+['bdgt_id']+[i.replace('_id','_name') for i in dim]+['cpp']]
    temp=temp.rename(columns={'cpp':'cps'})
    temp.to_csv('upload_cps.csv',index=False)
    
    # input_plan table
    print('Note: Creating input_plan table')
    temp=adm_data_var[mmm.get_dim_bdgt(client_path)+['bdgt_id','client_id']].drop_duplicates('bdgt_id')
    pd.io.sql.execute('delete from mmm_input_plan where client_id={}'.format(client_id),conn)
    temp.to_sql('mmm_input_plan',conn,index=False,if_exists='append',flavor='mysql')
    
    # modules and modules dim table
    print("Note: Creating Module Relalted Tables")
    # modules table
    temp=adm_modules.label
    tb_name='mmm_label_modules'
    label=pd.read_sql('select label from {}'.format(tb_name),conn).label.values
    temp=temp[~temp.isin(label)]
    if temp.shape[0]!=0:
        print('Note: New labels inserted in {}'.format(tb_name))
        temp=pd.DataFrame({'label':temp})
        temp.to_sql(tb_name,conn,index=False,if_exists='append',flavor='mysql')
    temp=pd.read_sql('select * from {}'.format(tb_name),conn)
    temp=pd.merge(adm_modules,temp,on='label',how='left')
    temp=temp.drop('label',axis=1).rename(columns={'id':'mmm_label_module_id'})
    temp['client_id']=client_id
    pd.io.sql.execute('delete from mmm_modules where client_id={}'.format(client_id),conn)
    temp.to_sql('mmm_modules',conn,index=False,if_exists='append',flavor='mysql')
    # modules_dim table
    temp=adm_modules_dim.label
    tb_name='mmm_label_modules_dim'
    label=pd.read_sql('select label from {}'.format(tb_name),conn).label.values
    temp=temp[~temp.isin(label)]
    if temp.shape[0]!=0:
        print('Note: New labels inserted in {}'.format(tb_name))
        temp=pd.DataFrame({'label':temp})
        temp.to_sql(tb_name,conn,index=False,if_exists='append',flavor='mysql')
    temp=pd.read_sql('select * from {}'.format(tb_name),conn)
    temp=pd.merge(adm_modules_dim,temp,on='label',how='left')
    temp=temp.drop('label',axis=1).rename(columns={'id':'{}_id'.format(tb_name)})
    temp['dim']=temp.dim.str.replace('_id','')
    temp['client_id']=client_id
    pd.io.sql.execute('delete from mmm_modules_dim where client_id={}'.format(client_id),conn)
    temp.to_sql('mmm_modules_dim',conn,index=False,if_exists='append',flavor='mysql')
    
    # modeliput_output table
    adm_output['client_id']=client_id
    pd.io.sql.execute('delete from mmm_modelinput_output where client_id={}'.format(client_id),conn)
    adm_output.to_sql('mmm_modelinput_output',conn,index=False,if_exists='append',flavor='mysql')
    
    # close db connection
    conn.close()
    print('Note: Done!')
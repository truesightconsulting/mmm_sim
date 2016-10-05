def mmm_main(client_path,main_path,mmm_id,client_id,is_staging,db_server,db_name,port,username,password):
#    client_path='c:/Users/XinZhou/Documents/GitHub/demo/'
#    main_path='c:/Users/XinZhou/Documents/GitHub/mmm_sim/'
#    mmm_id=1
#    client_id=31
#    # DB server info
#    is_staging=True
#    db_server="bitnami.cluster-chdidqfrg8na.us-east-1.rds.amazonaws.com"
#    db_server="127.0.0.1"
#    db_name="nviz"
#    port=3306
#    if is_staging:
#      username="root"
#      password="bitnami"
#    else:
#      username="Zkdz408R6hll"
#      password="XH3RoKdopf12L4BJbqXTtD2yESgwL$fGd(juW)ed"
    
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
    client_path='{}admin/mmm/'.format(client_path)
    os.chdir(client_path)
    # improt functions module
    sys.path.append(main_path)
    import mmm_modelinput_functions as mmm
    #importlib.reload(mmm)
    
    # load data
    print('Note: Loading data')
    engine=sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(username,password,db_server,db_name))
    conn=engine.connect()
    # convert modelinput_raw by adding id's
    modelinput_raw=pd.read_csv('adm_data_raw.csv')
    modelinput_raw['date']=pd.to_datetime(modelinput_raw.date,format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
    dim=modelinput_raw.columns[modelinput_raw.columns.str.contains('_name')].str.replace('_name','_id').tolist()
    for i in dim:
        #i=dim[0]
        tb_name='mmm_label_{}'.format(i.split('_id')[0])
        temp=pd.read_sql('select * from {}'.format(tb_name),conn)
        temp=temp.rename(columns={'id':i,'label':i.replace('_id','_name')})
        modelinput_raw=pd.merge(modelinput_raw,temp,on=i.replace('_id','_name'),how='left')
    # convert modelinput_var by adding names
    modelinput_var=pd.read_sql('select * from mmm_modelinput_var where client_id={}'.format(client_id),conn).drop(['id','client_id'],axis=1)
    modelinput_var=modelinput_var.dropna(how='all',axis=1)
    dim=modelinput_var.columns[modelinput_var.columns.str.contains('_id')].str.replace('_name','_id').tolist()
    dim.remove('bdgt_id')
    for i in dim:
        #i=dim[0]
        tb_name='mmm_label_{}'.format(i.split('_id')[0])
        temp=pd.read_sql('select * from {}'.format(tb_name),conn)
        temp=temp.rename(columns={'id':i,'label':i.replace('_id','_name')})
        modelinput_var=pd.merge(modelinput_var,temp,on=i,how='left')
    # load other tables
    modelinput_metric=pd.read_csv('adm_metric.csv')
    modules_dim=pd.read_csv('adm_modules_dim.csv')
    modelinput_output=pd.read_sql('select * from mmm_modelinput_output where client_id={}'.format(client_id),conn).drop(['id','client_id'],axis=1)
    def load_userinput(x):
        return pd.read_sql('select * from {} where mmm_id={}'.format(x,mmm_id),conn).dropna(how='all',axis=1).drop(['id','mmm_id'],axis=1)
    input_dim_chan=load_userinput('mmm_userinput_dim_chan')
    input_dim_dma=load_userinput('mmm_userinput_dim_dma')
    input_dim_sales=load_userinput('mmm_userinput_dim_sales')
    input_dim_salchan=load_userinput('mmm_userinput_dim_salchan')
    input_cps=load_userinput('mmm_userinput_cps')
    # fill input data for missing dates
    input_plan=load_userinput('mmm_userinput_plan')
    date_min=pd.to_datetime(pd.read_sql('select date_min from mmm_input_setup where client_id={}'.format(client_id),conn).date_min,format='%Y-%m-%d').min()
    date_start=pd.to_datetime(input_plan.date,format='%Y-%m-%d').min()
    date_missing=pd.date_range(date_min,date_start,freq='7D').strftime('%Y-%m-%d').tolist()
    if len(date_missing)>1:
        del date_missing[-1]
    temp=input_plan.drop_duplicates(subset='bdgt_id').drop(['date','value'],axis=1)
    n=temp.shape[0]
    temp=temp.ix[temp.index.tolist()*len(date_missing)].copy()
    temp['date']=np.repeat(np.array(date_missing),n)
    temp['value']=0
    input_plan=pd.concat([input_plan,temp],axis=0)
    conn.close()
    
    # filter var with dim input data
    print('Note: Filting variables')
    dim=mmm.get_dim(client_path)
    for i in dim:
        #i=dim[2]
        expr = "input_dim_{}.copy()".format(i)
        temp=eval(parser.expr(expr).compile())
        key=mmm.get_dim_n(i,client_path)
        modelinput_var=pd.merge(modelinput_var,temp[key],on=key,how='inner')
        
    # cps update
    modelinput_var=pd.merge(modelinput_var,input_cps[['bdgt_id','cps']],on='bdgt_id',how='left')
    modelinput_var.ix[modelinput_var.cps.isnull(),'cps']=modelinput_var.cpp[modelinput_var.cps.isnull()]
    
    # process input plan data
    print('Note: Processing plan')
    input_plan=input_plan.fillna(0)
    date=input_plan.date.unique()
    dim_dma=mmm.get_dim_n('dma',client_path)
    temp_modelinput_var=modelinput_var.drop_duplicates(subset=['bdgt_id'])
    def stack_plan(i):
        #i=date[0]
        list_temp=dim_dma+['date','value','bdgt_id']
        temp=input_plan.ix[input_plan.date==i,list_temp].copy()
        temp=pd.merge(temp,temp_modelinput_var[['bdgt_id','var','apprate','ratio']],on='bdgt_id',how='inner')
        temp['value']=temp['apprate']*temp['ratio']*temp['value']
        list_temp=dim_dma+['date','value','var']
        return temp[list_temp].copy()
    
    temp=pd.concat(list(map(stack_plan,date)))
    list_temp=dim_dma+['date']
    temp=pd.pivot_table(temp,index=list_temp,columns='var',values='value',fill_value=0)
    temp.reset_index(inplace=True)
    dim_var=modelinput_var['var'].unique().tolist()
    list_temp=['date']+dim_dma+dim_var
    temp=pd.concat([modelinput_raw[list_temp],temp],axis=0,ignore_index=True)
    temp['date']=pd.to_datetime(temp.date,format='%Y-%m-%d')
    list_temp=dim_dma+['date']
    for_decomp=temp.sort_values(by=list_temp,axis=0)
    
    # adstock and decomp
    print('Note: Forecasting')
    for_decomp['group_dma']=for_decomp[dim_dma].apply(lambda x: '+'.join(x.astype('U')),axis=1)
    modelinput_var['group_dma']=modelinput_var[dim_dma].apply(lambda x: '+'.join(x.astype('U')),axis=1)
    
    dim_model=mmm.get_dim_model(client_path)
    modelinput_var['group_model']=modelinput_var[dim_model].apply(lambda x: '+'.join(x.astype('U')),axis=1)
    
    group_dma=for_decomp.group_dma.unique()
    group_model=modelinput_var.group_model.unique()
    f_name=mmm.get_f_name(client_path)
    
    def decomp(x):
        i=group_dma[0]
        j=group_model[0]
        i=x[0]
        j=x[1]
        temp_var=modelinput_var.ix[(modelinput_var.group_dma==i) & (modelinput_var.group_model==j)].copy()
        dim_var_temp=temp_var['var'].tolist()
        list_temp=dim_var_temp+['date']
        temp=for_decomp.ix[for_decomp.group_dma==i,list_temp].copy()
        return mmm.decomp_f(f_name)(temp,temp_var,i,j)
        
    temp=pd.concat(list(map(decomp,itertools.product(group_dma,group_model))))
    temp=temp.fillna(0)
    
    # output
    print('Note: Manipulating data for output')
    temp['date']=temp.date.dt.strftime('%Y-%m-%d')
    dim_date=temp.date.unique().tolist()
    temp=pd.melt(temp,id_vars=['group_dma','group_model','date'],var_name='var')
    temp=pd.pivot_table(temp,index=['group_dma','group_model','var'],columns='date',values='value',fill_value=0)
    temp.reset_index(inplace=True)
    for_output=pd.merge(modelinput_var,temp,on=['group_dma','group_model','var'],how='inner')
    for_output['all_id']=1
    for_output['all_name']='overall'
    dim_metric=mmm.get_dim_metric(client_path)
    dim_dim=for_output.columns.to_series()[for_output.columns.to_series().str.contains('_id|_name')].tolist()
    list_temp=dim_dim+dim_metric+dim_date
    for_output=pd.melt(for_output[list_temp],id_vars=dim_dim+dim_metric,value_vars=dim_date,var_name='week_name')
    for i in range(modelinput_metric.shape[0]):
        for_output[modelinput_metric.label[i]]=for_output[modelinput_metric.metric[i]]*for_output.value
    for_output['week_id']=for_output.week_name
    temp=input_plan.rename(columns={'value':'Spend','date':'week_id'})
    temp['week_id']=pd.to_datetime(temp.week_id,format='%Y-%m-%d')
    temp['week_id']=temp.week_id.dt.strftime('%Y-%m-%d')
    for_output=pd.merge(for_output,temp[['bdgt_id','week_id','Spend']],on=['bdgt_id','week_id'],how='inner')
    for_output=for_output.ix[~(for_output.week_id.isin(date_missing))].copy()
    for_output=for_output.fillna(0)
    dim_metric=modelinput_metric.label.tolist()
    
    # rolling up
    print('Note: Aggregating output')
    dim_bdgt=mmm.get_dim_bdgt(client_path)+['week_id']
    list_temp=dim_bdgt+['Spend','bdgt_id']
    summary_sp=for_output.drop_duplicates(subset=['bdgt_id','week_id'])[list_temp]
    summary={i:None for i in modelinput_output.label}
    modelinput_output['json']=''
    for i in range(modelinput_output.shape[0]):
        #i=1
        # create output table
        dim=modelinput_output.dim[i].split(',')
        dim1=dim+[i.replace('_id','_name') for i in dim]
        dim_sp=[i for i in dim if i in dim_bdgt]
        summary_npv=for_output.groupby(dim1,as_index=False)[dim_metric].agg(np.sum)
        if dim_sp==[]:
            summary_sp1=summary_sp['Spend'].sum()
            temp=summary_npv
            temp['Spend']=summary_sp1
        else:
            summary_sp1=summary_sp.groupby(dim_sp,as_index=False)['Spend'].agg(np.sum)
            temp=pd.merge(summary_npv,summary_sp1,on=dim_sp,how='left')
    
        # round numbers
        temp[dim_metric+['Spend']]=temp[dim_metric+['Spend']].round(0)
        
        # re-order columns
        temp=temp[dim1+['Spend']+dim_metric]
    
        # drop id columns
        drop_col=temp.columns.to_series()[temp.columns.str.contains('_id')]
        temp=temp.drop(drop_col,axis=1)
        if 'all_id' in dim:
            temp=temp.drop('all_name',axis=1)
        
        # rename columns
        if modelinput_output.type[i]=='excel':
            modules_dim.dim=modules_dim.dim.str.replace('_id','_name')
            dim_excel={modules_dim.dim[i]:modules_dim.label[i] for i in range(modules_dim.shape[0])}
            dim_excel['week_name']='Week'
            temp=temp.rename(columns=dim_excel)
        summary[modelinput_output.label[i]]=temp
        # convert to json
        modelinput_output.ix[i,'json']=temp.to_json(orient='records')
    
    # convert to json
    modelinput_output['mmm_id']=mmm_id
    group=modelinput_output.drop_duplicates(subset=['group','chart'])[['group','chart']]
    group=group.reset_index().drop('index',axis=1)
    def to_json(i):
        #i=5
        index=(modelinput_output.group==group.group[i]) & (modelinput_output.chart==group.chart[i])
        temp=modelinput_output.ix[index].copy()
        temp=temp.reset_index()
        if temp.drilldown.tolist()[0]==1:
            list_temp=['"{}":{}'.format(temp.label[j].split('_')[1],temp.json[j]) for j in range(temp.shape[0])]
            temp.ix[0,'json']='{'+','.join(list_temp)+'}'
            temp=temp.ix[[0]].copy()
        elif group.chart[i]=='area':
            temp_type=temp.type[0]
            temp_prefix=[list(filter(lambda x: temp_type in x,temp.dim[j].split(',')))[0] for j in range(temp.shape[0])]
            temp_dim=pd.read_csv('adm_modules_dim.csv',index_col='dim')
            temp_prefix=temp_dim.ix[temp_prefix].label.tolist()
            list_temp=['"{}":{}'.format(temp_prefix[j],temp.json[j]) for j in range(temp.shape[0])]
            temp.ix[0,'json']='{'+','.join(list_temp)+'}'
            temp=temp.ix[[0]].copy()
        temp=temp.drop(['label','index','dim'],axis=1)
        return temp
        
    temp=pd.concat(list(map(to_json,range(group.shape[0]))),axis=0)
    temp=temp.rename(columns={'group':'label'})
    temp.ix[temp.label=='overall','json']=temp.json[temp.label=='overall'].str.replace('\\[|\\]','')
    output=temp
    
    # upload output to db
    print('Note: Uploading to DB')
    conn=engine.connect()
    pd.io.sql.execute('delete from mmm_output where mmm_id={}'.format(mmm_id),conn)
    output.to_sql('mmm_output',conn,index=False,if_exists='append',flavor='mysql')
    conn.close()
import pandas as pd
import numpy as np
def get_dim(client_path):
    adm_setup=pd.read_csv('{}/adm_setup.csv'.format(client_path))
    index=adm_setup['attribute'].str.contains('dim_')
    return adm_setup.ix[index,'attribute'].str.slice(4).tolist()
    
def get_dim_n(x,client_path):
    #x='chan'
    adm_setup=pd.read_csv('{}/adm_setup.csv'.format(client_path))
    index=adm_setup['attribute']=='dim_{}'.format(x)
    return adm_setup.ix[index,'value'].tolist()[0].split(',')
    
def get_dim_model(client_path):
    #x='chan'
    adm_setup=pd.read_csv('{}/adm_setup.csv'.format(client_path))
    index=adm_setup['attribute']=='model_dim'
    return adm_setup.ix[index,'value'].tolist()[0].split(',')
    
def get_dim_metric(client_path):
    #x='chan'
    adm_setup=pd.read_csv('{}/adm_setup.csv'.format(client_path))
    index=adm_setup['attribute']=='metric'
    return adm_setup.ix[index,'value'].tolist()[0].split(',')

def get_f_name(client_path):
    #x='chan'
    adm_setup=pd.read_csv('{}/adm_setup.csv'.format(client_path))
    index=adm_setup['attribute']=='formula'
    return adm_setup.ix[index,'value'].tolist()[0]

def get_dim_bdgt(client_path):
    #x='chan'
    adm_setup=pd.read_csv('{}/adm_setup.csv'.format(client_path))
    index=adm_setup['attribute']=='bdgt_dim'
    return adm_setup.ix[index,'value'].tolist()[0].split(',')

def decomp_f(name):
    if name=='ninah':
        def f(x,var,group_dma,group_model):
            '''
            x is the pannel data, df class
            var is para df
            '''
#            x=temp
#            var=temp_var
            date=x.date
            x=x.drop('date',axis=1).as_matrix()
            decay=np.exp(np.log(0.5)/var.hl)
            learn=-10*np.log(1-var.hrf)
            temp=np.zeros_like(x)
            for i in range(temp.shape[0]):
                if i==0:
                    temp[0]=(1-(1/np.exp(learn*(x[i]/var.cps)/var['max'])))
                else:
                    temp[i]=(1-((1-temp[i-1]*decay)/np.exp(learn*(x[i]/var.cps)/var['max'])))
            temp=pd.DataFrame(var.beta.as_matrix()*temp,columns=var['var'])
            temp['date']=date
            temp['group_dma']=group_dma
            temp['group_model']=group_model
            return temp       
    return f
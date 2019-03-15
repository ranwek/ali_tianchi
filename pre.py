#coding=utf-8
import pandas as pd



if __name__ == '__main__':
    # read data
    label='c'
    print ('read data...')
    app_resources = pd.read_csv('../data/app_resources.csv',header=None)
    machine_resources = pd.read_csv('../data/machine_resources.'+label+'.csv',header=None)
    instance_deploy = pd.read_csv('../data/instance_deploy.'+label+'.csv',header=None)
    app_interference = pd.read_csv('../data/app_interference.csv',header=None)
    #job_info = pd.read_csv('../data/job_info.'+label+'.csv',header=None)
    
    #fill nan  
    instance_deploy=instance_deploy.fillna('machine_-1')
    
    #rename
    instance_deploy.columns=['inst_id','app_id','machine_id']
    app_resources.columns=['app_id','cpu','mem','disk','P','M','PM']
    machine_resources.columns=['machine_id','cpu','mem','disk','P','M','PM']
    app_interference.columns=['app_id_1','app_id_2','max']
    
    #merge
    print ('merge...')
    instance_deploy=pd.merge(instance_deploy,app_resources,on='app_id',how='left')
    
    #app_interference
    app_interference['app_id_1']=app_interference['app_id_1'].str.split('_',expand=True)[1]
    app_interference['app_id_2']=app_interference['app_id_2'].str.split('_',expand=True)[1]
    #print(app_interference.loc[5])
    
    #app_info
    app_resources['app_id']=app_resources['app_id'].str.split('_',expand=True)[1]
    cpu=app_resources['cpu'].str.split('|',expand=True)
    mem=app_resources['mem'].str.split('|',expand=True)
    cpu.columns=['cpu'+str(i) for i in range(len(cpu.columns))]
    mem.columns=['mem'+str(i) for i in range(len(mem.columns))]
    app_resources=app_resources.drop(['cpu', 'mem'], axis=1)
    app_resources=pd.concat([app_resources,cpu,mem],axis=1)
    
    #instance_info
    instance_deploy['inst_id']=instance_deploy['inst_id'].str.split('_',expand=True)[1]
    instance_deploy['app_id']=instance_deploy['app_id'].str.split('_',expand=True)[1]
    instance_deploy['machine_id']=instance_deploy['machine_id'].str.split('_',expand=True)[1]
    cpu=instance_deploy['cpu'].str.split('|',expand=True)
    mem=instance_deploy['mem'].str.split('|',expand=True)
    cpu.columns=['cpu'+str(i) for i in range(len(cpu.columns))]
    mem.columns=['mem'+str(i) for i in range(len(mem.columns))]
    instance_deploy=instance_deploy.drop(['cpu', 'mem'], axis=1)
    instance_info=pd.concat([instance_deploy,cpu,mem],axis=1)
    #print (instance_info.loc[5])
    
    #machine_info
    for i in range(len(cpu.columns)):
        machine_resources[cpu.columns[i]]=machine_resources['cpu']
    for i in range(len(mem.columns)):
        machine_resources[mem.columns[i]]=machine_resources['mem']
    machine_resources.drop(['cpu', 'mem'], axis=1)
    machine_resources['machine_id']=machine_resources['machine_id'].str.split('_',expand=True)[1]
    #print (machine_resources.loc[5])
    
    #output
    print ('2csv...')
    app_resources.to_csv(r'../data/my_app_info.csv',sep=',',index=False )
    machine_resources.to_csv(r'../data/my_machine_resources.csv',sep=',',index=False )
    instance_info.to_csv(r'../data/my_instance_info.csv',sep=',',index=False)
    app_interference.to_csv(r'../data/my_app_interference.csv',sep=',',index=False)
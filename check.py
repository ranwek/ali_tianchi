#coding=utf-8
import pandas as pd
import numpy as np

if __name__ == '__main__':
    machine_resources_data = pd.read_csv('../data/my_machine_resources.csv')
    instance_info_data = pd.read_csv('../data/my_instance_info.csv')
    app_interference_data = pd.read_csv('../data/my_app_interference.csv')
    submit = pd.read_csv('../submit/submit_20180817_193203.csv',header=None)
    cpu_columns=['cpu'+str(i) for i in range(98)]
    mem_columns=['mem'+str(i) for i in range(98)]
    all_columns=cpu_columns+mem_columns+['disk','P','M']
    T=98
    alpha=10
    beta=0.5
    
    app_max=instance_info_data['app_id'].max()
    app_interference_dict={}
    app_interference=app_interference_data.values
    for i in range(len(app_interference)):
        app_interference_dict[app_interference[i,0]*app_max+app_interference[i,1]]=app_interference[i,2]
        
    submit=submit[[1,2]]
    submit.columns=[0,1]
    submit[0]=submit[0].str.split('_',expand=True)[1].astype(int)
    submit[1]=submit[1].str.split('_',expand=True)[1].astype(int)
    submit.columns=['inst_id','new_machine_id']
    #print (submit['inst_id'])
    submit=pd.merge(instance_info_data[['inst_id','machine_id']],submit,on='inst_id',how='left')
    instance_info_data['machine_id'][submit['new_machine_id']>0]=submit['new_machine_id'][submit['new_machine_id']>0]
    

  
    machine_resources=machine_resources_data[all_columns].values
    instance_info=instance_info_data[all_columns].values
    deploy_inst=instance_info_data[instance_info_data['machine_id']>0]
    deploy_machine=deploy_inst['machine_id'].unique()
    if min(instance_info_data['machine_id'].unique())<0:
        print ("部分实例没有部署")
    else :
        print ("全部实例已部署")
    
    score=np.zeros((len(deploy_machine),98))
    
    for i in range(len(deploy_machine)):
        inst_in_machine=deploy_inst['app_id'][deploy_inst['machine_id']==deploy_machine[i]]       
        for j in range(len(inst_in_machine)):
            for k in range(j):
                #print (inst_in_machine[inst_in_machine.index[k]])
                if app_interference_dict.get(inst_in_machine[inst_in_machine.index[k]]*app_max+inst_in_machine[inst_in_machine.index[j]])!=None:
                    if app_interference_dict.get(inst_in_machine[inst_in_machine.index[k]]*app_max+inst_in_machine[inst_in_machine.index[j]]) \
                            < len(inst_in_machine[inst_in_machine==inst_in_machine[inst_in_machine.index[j]]])-\
                            int(inst_in_machine[inst_in_machine.index[k]]==inst_in_machine[inst_in_machine.index[j]]):
                        print ("应用冲突：",inst_in_machine[inst_in_machine.index[k]],inst_in_machine[inst_in_machine.index[j]],\
                               app_interference_dict.get(inst_in_machine[inst_in_machine.index[k]]*app_max+inst_in_machine[inst_in_machine.index[j]])\
                            , len(inst_in_machine[inst_in_machine==inst_in_machine[inst_in_machine.index[j]]]))
                        print (instance_info_data['machine_id'].loc[inst_in_machine.index[j]],\
                               instance_info_data['inst_id'].loc[inst_in_machine.index[j]])
                if app_interference_dict.get(inst_in_machine[inst_in_machine.index[k]]+inst_in_machine[inst_in_machine.index[j]]*app_max)!=None:
                    if app_interference_dict.get(inst_in_machine[inst_in_machine.index[k]]+inst_in_machine[inst_in_machine.index[j]]*app_max) \
                            < len(inst_in_machine[inst_in_machine==inst_in_machine[inst_in_machine.index[k]]])-\
                            int(inst_in_machine[inst_in_machine.index[k]]==inst_in_machine[inst_in_machine.index[j]]):
                        print ("应用冲突：",inst_in_machine[inst_in_machine.index[j]],inst_in_machine[inst_in_machine.index[k]],\
                               app_interference_dict.get(inst_in_machine[inst_in_machine.index[k]]+inst_in_machine[inst_in_machine.index[j]]*app_max) \
                            , len(inst_in_machine[inst_in_machine==inst_in_machine[inst_in_machine.index[j]]]))
                        print (instance_info_data['machine_id'].loc[inst_in_machine.index[k]],\
                               instance_info_data['inst_id'].loc[inst_in_machine.index[k]])
    
    machine_resources=machine_resources_data[all_columns].values.astype(float)
    machine_num=np.zeros((len(machine_resources),1))
    machine_number={}
    for i in range(len(machine_resources)):
        machine_number[machine_resources_data['machine_id'].iloc[i]]=i
        
    for j in range(len(instance_info_data)):
        if (instance_info_data['machine_id'].iloc[j]>0):
            i=machine_number[instance_info_data['machine_id'].iloc[j]]
            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
            machine_num[i]+=1
    
    alpha=machine_num+1
    if np.min(machine_resources)<0:
        print("空间不足")
    deploy_machine=np.unique(instance_info_data['machine_id'].values)
    score=machine_resources[:,0:T]/machine_resources_data[cpu_columns].values.astype(float)
    score=1-score-beta
    score[score<0]=0
    print ("最后得分：",sum(sum(1+alpha*(np.exp(score)-1)))/98+len(deploy_machine)-len(machine_resources))
    machine_score=np.sum(1+alpha*(np.exp(score)-1),axis=1)/98
    
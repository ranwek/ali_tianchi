#coding=utf-8
import pandas as pd
import numpy as np





if __name__ == '__main__':    
    machine_resources_data = pd.read_csv('../data/my_machine_resources.csv')
    instance_info_data = pd.read_csv('../data/my_instance_info.csv')
    app_interference_data = pd.read_csv('../data/my_app_interference.csv')
    submit = pd.read_csv('../submit/submit_20180905_123919.csv',header=None)
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
    submit_dict={}
    for i in range(len(submit)):
        submit_dict[submit[0].iloc[i]]=i
    submit=submit.iloc[(list(submit_dict.values()))]
    submit[0]=submit[0].str.split('_',expand=True)[1].astype(int)
    submit[1]=submit[1].str.split('_',expand=True)[1].astype(int)
    submit.columns=['inst_id','new_machine_id']
    #print (submit['inst_id'])
    submit=pd.merge(instance_info_data[['inst_id','machine_id']],submit,on='inst_id',how='left')
    instance_info_data['machine_id'][submit['new_machine_id']>0]=submit['new_machine_id'][submit['new_machine_id']>0]
    
    move = pd.read_csv('../submit/move.csv',header=None)
    move=move[[1,2]]
    move.columns=[0,1]
    move_dict={}
    for i in range(len(move)):
        move_dict[move[0].iloc[i]]=i
    move=move.iloc[(list(move_dict.values()))]
    move[0]=move[0].str.split('_',expand=True)[1].astype(int)
    move[1]=move[1].str.split('_',expand=True)[1].astype(int)
    move.columns=['inst_id','new_machine_id']
    #print (submit['inst_id'])
    move=pd.merge(instance_info_data[['inst_id','machine_id']],move,on='inst_id',how='left')
    instance_info_data['machine_id'][move['new_machine_id']>0]=move['new_machine_id'][move['new_machine_id']>0]
  
    machine_resources=machine_resources_data[all_columns].values
    instance_info=instance_info_data[all_columns].values
    deploy_inst=instance_info_data[instance_info_data['machine_id']>0]
    deploy_machine=deploy_inst['machine_id'].unique()
    if min(instance_info_data['machine_id'].unique())<0:
        print ("部分实例没有部署")
    else :
        print ("全部实例已部署")
    
    score=np.zeros((len(deploy_machine),98))
    

    
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
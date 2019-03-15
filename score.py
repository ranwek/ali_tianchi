#coding=utf-8
import pandas as pd
import numpy as np





if __name__ == '__main__':    
    #calculate score
    machine_resources_data = pd.read_csv('../data/my_machine_resources.csv')
    instance_info_data = pd.read_csv('../data/my_instance_info.csv')
    app_interference_data = pd.read_csv('../data/my_app_interference.csv')
    file_name='../submit/submit_20180817_183011.csv'
    submit = pd.read_csv(file_name,header=None)
    
    cpu_columns=['cpu'+str(i) for i in range(98)]
    mem_columns=['mem'+str(i) for i in range(98)]
    all_columns=cpu_columns+mem_columns+['disk','P','M']
    T=98
    alpha=10
    beta=0.5
    
    submit=submit[[1,2]]
    submit.columns=[0,1]
    submit[0]=submit[0].str.split('_',expand=True)[1].astype(int)
    submit[1]=submit[1].str.split('_',expand=True)[1].astype(int)
    submit.columns=['inst_id','new_machine_id']
    submit=pd.merge(instance_info_data[['inst_id','machine_id']],submit,on='inst_id',how='left')
    instance_info_data['machine_id'][submit['new_machine_id']>0]=submit['new_machine_id'][submit['new_machine_id']>0]
    machine_resources=machine_resources_data[all_columns].values.astype(float)
    instance_info=instance_info_data[all_columns].values
    machine_instance_list=np.zeros((len(instance_info),2))
    machine_num=np.zeros((len(machine_resources),1))

    for j in range(len(instance_info_data)):
        if (instance_info_data['machine_id'].iloc[j]>0):
            i=np.nonzero(machine_resources_data['machine_id']==int(instance_info_data['machine_id'].iloc[j]))
            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
            machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
            machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]
            machine_num[i]+=1
    
    alpha=machine_num+1
    if np.min(machine_resources)<0:
        print("空间不足")
    deploy_machine=np.unique(machine_instance_list[:,1])
    score=machine_resources[:,0:T]/machine_resources_data[cpu_columns].values.astype(float)
    score=1-score-beta
    score[score<0]=0
    print ("最后得分：",(np.sum(1+alpha*(np.exp(score)-1)))/98+len(deploy_machine)-len(machine_resources))
    machine_score=np.sum(1+alpha*(np.exp(score)-1),axis=1)/98

#coding=utf-8
import pandas as pd
import numpy as np
import random

        
if __name__ == '__main__':    
    # read data 
    label='c'
    num_of_no_capacity=0
    file_name='../data/job_info.'+label+'.csv'
    job_deploy='../submit/job.csv'
    submit_job = pd.read_csv(job_deploy,header=None)
    submit_job[1]=submit_job[1].str.split('_',expand=True)[1].astype(int)
    
    job_dict={}
    for line in open(file_name):
        job_dict[line[:-1].split(',')[0]]=line[:-1].split(',')
    
    job_cpu_cost=0
    job_num=0
    job_deploy={}
    for each_job_key in job_dict.keys():
        job_cpu_cost+=float(job_dict[each_job_key][1])*float(job_dict[each_job_key][3])*float(job_dict[each_job_key][4])
        job_num+=int(job_dict[each_job_key][3])
    
     
    
    #calculate score
    machine_resources_data = pd.read_csv('../data/my_machine_resources.csv')
    instance_info_data = pd.read_csv('../data/my_instance_info.csv')
    app_interference_data = pd.read_csv('../data/my_app_interference.csv')
    file_name='../submit/submit_20180905_123919.csv'
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
    
    machine_resources=machine_resources_data[all_columns].values.astype(float)
    instance_info=instance_info_data[all_columns].values
    machine_instance_list=np.zeros((len(instance_info),2))
    machine_num=np.zeros((len(machine_resources),1))
    
    machine_number={}
    for i in range(len(machine_resources)):
        machine_number[machine_resources_data['machine_id'].iloc[i]]=i
        
    for j in range(len(instance_info_data)):
        if (instance_info_data['machine_id'].iloc[j]>0):
            i=machine_number[instance_info_data['machine_id'].iloc[j]]
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
    
    deploy_machine=np.unique(list(instance_info_data['machine_id'].values) +list(submit_job[1].values))
    job_cpu_cost=0
    job_num=0
    job_deploy={}
    for each_job_key in job_dict.keys():
        job_cpu_cost+=float(job_dict[each_job_key][1])*float(job_dict[each_job_key][3])*float(job_dict[each_job_key][4])
        job_num+=int(job_dict[each_job_key][3])
    
    #machine_resources[:,0:T]-=machine_resources_data[cpu_columns].values*0.5
    machine_resources_cpu_job=np.zeros((len(machine_resources),15*T))
    machine_resources_mem_job=np.zeros((len(machine_resources),15*T))
    for i in range(len(machine_resources)):
        for j in range(T):
            machine_resources_cpu_job[i,j*15:j*15+15]=machine_resources[i,j]
            machine_resources_mem_job[i,j*15:j*15+15]=machine_resources[i,j+T]
    init_machine_resources_cpu_job=np.zeros((len(machine_resources),15*T))
    for i in range(len(init_machine_resources_cpu_job)):
        init_machine_resources_cpu_job[i,:]=machine_resources_data['cpu'].iloc[i]
            
    for i in range(len(submit_job)):
        machine_resources_cpu_job[ (machine_number[submit_job[1].iloc[i]]),submit_job[2].iloc[i]:submit_job[2].iloc[i]+int(job_dict[submit_job[0].iloc[i]][4])]-=float(job_dict[submit_job[0].iloc[i]][1])*int(submit_job[3].iloc[i])
        
    alpha=machine_num+1
    if np.min(machine_resources_cpu_job)<0:
        print("空间不足")
    score=machine_resources_cpu_job/init_machine_resources_cpu_job
    score=1-score-beta
    score[score<0]=0
    print ("最后得分：",(np.sum(1+alpha*(np.exp(score)-1)))/98/15+len(deploy_machine)-len(machine_resources))
    machine_score=np.sum(1+alpha*(np.exp(score)-1),axis=1)/98/15
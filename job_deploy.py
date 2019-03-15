#coding=utf-8
import pandas as pd
import numpy as np
import random

# deploy job

def mydot(matrix_input,n,value):
    matrix_output=np.zeros((len(matrix_input[0])-n+1,len(matrix_input)))
    n_value=np.ones((n,1))*value
    for i in range(len(matrix_input[0])-n+1):
        matrix_output[i]=np.dot(matrix_input[:,i:i+n],n_value).T
    return matrix_output.T

#类似卷积的操作
def mydot_individual(matrix_input,n,value):
    matrix_output=np.zeros((len(matrix_input)-n+1,len(matrix_input)))
    n_value=np.ones((n,1))*value
    for i in range(len(matrix_input)-n+1):
        matrix_output[i]=np.dot(matrix_input[i:i+n],n_value).T
    return matrix_output.T

def deploy(key):
    global job_dict,job_deploy,machine_resources_cpu_job,machine_resources_mem_job,deploy_machine,machine_number
    if job_deploy.get(key)==None:
        for predecessor_key in job_dict[key][5:]:
            if job_dict.get(predecessor_key)!=None:
                # deploy predecessor
                deploy(predecessor_key)
        job_deploy[key]=1
        #deployment
        job_name=key
        job_time=int(job_dict.get(key)[4])
        job_cpu=float(job_dict.get(key)[1])
        job_mem=float(job_dict.get(key)[2])
        job_n=int(job_dict.get(key)[3])
        random.shuffle(deploy_machine)
        #search
        for i in range(len(deploy_machine)):
            #找到可以部署的合适的位置
            point=np.argmax(mydot_individual(machine_resources_cpu_job[machine_number[deploy_machine[i]]],job_time,job_cpu))
            #该位置最大部署数量
            max_deploy_job=int(min(np.min(machine_resources_cpu_job[machine_number[deploy_machine[i]],point:point+job_time]/job_cpu),\
                               np.min(machine_resources_mem_job[machine_number[deploy_machine[i]],point:point+job_time]/job_mem)))
            if max_deploy_job>=job_n:
                #如果可以部署
                if job_n>0:
                    machine_resources_cpu_job[machine_number[deploy_machine[i]],point:point+job_time]-=job_n*job_cpu
                    machine_resources_mem_job[machine_number[deploy_machine[i]],point:point+job_time]-=job_n*job_mem
                    print(job_name+',machine_'+str(int(deploy_machine[i]))+','+str(point)+','+str(job_n))
                    break
            else:
                #如果可以部署
                if max_deploy_job>0:
                    machine_resources_cpu_job[machine_number[deploy_machine[i]],point:point+job_time]-=max_deploy_job*job_cpu
                    machine_resources_mem_job[machine_number[deploy_machine[i]],point:point+job_time]-=max_deploy_job*job_mem
                    job_n-=max_deploy_job
                    print(job_name+',machine_'+str(int(deploy_machine[i]))+','+str(point)+','+str(max_deploy_job))
        print (job_dict.get(key))
        

if __name__ == '__main__':    
    #calculate score
    machine_resources_data = pd.read_csv('../data/my_machine_resources.csv')
    instance_info_data = pd.read_csv('../data/my_instance_info.csv')
    app_interference_data = pd.read_csv('../data/my_app_interference.csv')
    file_name='../submit/submit_20180817_193203.csv'
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
    # read data
    
    label='a'
    file_name='../data/job_info.'+label+'.csv'
    job_dict={}
    for line in open(file_name):
        job_dict[line[:-1].split(',')[0]]=line[:-1].split(',')
    
    job_cpu_cost=0
    job_num=0
    job_deploy={}
    for each_job_key in job_dict.keys():
        job_cpu_cost+=float(job_dict[each_job_key][1])*float(job_dict[each_job_key][3])*float(job_dict[each_job_key][4])
        job_num+=int(job_dict[each_job_key][3])
    
    machine_resources[:,0:T]-=machine_resources_data[cpu_columns].values*0.5
    machine_resources_cpu_job=np.zeros((len(machine_resources),15*T))
    machine_resources_mem_job=np.zeros((len(machine_resources),15*T))
    for i in range(len(machine_resources)):
        for j in range(T):
            machine_resources_cpu_job[i,j*15:j*15+15]=machine_resources[i,j]
            machine_resources_mem_job[i,j*15:j*15+15]=machine_resources[i,j+T]
            
    for each_job_key in job_dict.keys():
        deploy (each_job_key)


        
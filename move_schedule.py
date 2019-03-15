#coding=utf-8
import pandas as pd
import time
import numpy as np
import math

def mycov1(machine_resources,instance_info):
    machine_resources=machine_resources[:,0:98]
    instance_info=instance_info[0:98]
    result=(np.dot(machine_resources,instance_info.T))*0.1
    machine_resources=(machine_resources.T-np.mean(machine_resources,axis=1).T).T
    instance_info=instance_info-np.mean(instance_info)
    result+=(np.dot(machine_resources,instance_info.T))
    return result

def mycov(machine_resources,instance_info):
    global machine_num,init_machine_resources,point_of_machine
    T=98
    alpha=machine_num[0:point_of_machine]+1
    beta=0.5
    score=machine_resources[:,0:T]/init_machine_resources[:,0:T]
    score=1-score-beta
    score[score<0]=0
    result=np.sum(1+alpha*(np.exp(score)-1),axis=1)/T
    print(np.sum(1+alpha*(np.exp(score)-1))/T)
    alpha=machine_num[0:point_of_machine]+2
    score=(machine_resources[:,0:T]-instance_info[0:T])/init_machine_resources[:,0:T]
    score=1-score-beta
    score[score<0]=0
    result-=np.sum(1+alpha*(np.exp(score)-1),axis=1)/T
    return result

def more_machine(n):
    return 1


if __name__ == '__main__':
    #read data
    start = time.time()
    machine_resources_data = pd.read_csv('../data/my_machine_resources.csv')
    instance_info_data = pd.read_csv('../data/my_instance_info.csv').sample(frac=1).reset_index(drop=True)
    app_interference_data = pd.read_csv('../data/my_app_interference.csv')
    #file_name='../submit/'+'submit.csv'
    file_name='../submit/'+'move.csv'
    submit = pd.read_csv('../submit/submit_20180905_123919.csv',header=None)
    file_reulst=open(file_name,'w')
    
    #参数
    T=98
    alpha=10
    beta=0.5
    cpu_limits_1=1
    cpu_limits_2=1
    Sorting_threshold=0.7
    #more_machine=0.1
    
    cpu_columns=['cpu'+str(i) for i in range(T)]
    mem_columns=['mem'+str(i) for i in range(T)]
    all_columns=cpu_columns+mem_columns+['disk','P','M']
    
    #Reverse order of machine resources
    machine_resources_data=machine_resources_data.iloc\
    [np.argsort(machine_resources_data['disk'].values)[::-1]]
    machine_resources=machine_resources_data[all_columns].values.astype(float)
    mid_machine=3000
    machine_resources[0:mid_machine,0:T]=machine_resources[0:mid_machine,0:T]*cpu_limits_1
    machine_resources[mid_machine:,0:T]=machine_resources[mid_machine:,0:T]*cpu_limits_2
    
    
    #app interference dict
    app_max=instance_info_data['app_id'].max()
    app_interference_dict={}
    app_interference=app_interference_data.values
    for i in range(len(app_interference)):
        app_interference_dict[app_interference[i,0]*app_max\
                              +app_interference[i,1]]=app_interference[i,2]
        
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
    

    
    #dataframe -> np.array
    instance_info=instance_info_data[all_columns].values.astype(float)
    
    #convert to percent
    instance_info=instance_info/machine_resources[0,:]
    machine_resources=machine_resources/machine_resources[0,:]
    init_machine_resources=machine_resources.copy()
    
    #instance deployment list
    machine_instance_list=np.zeros((len(instance_info),2))
    machine_instance_list_update=np.zeros((len(instance_info),2))
    
    #number of deployed machine
    
    machine_number={}
    for i in range(len(machine_resources)):
        machine_number[machine_resources_data['machine_id'].iloc[i]]=i
        
    #number of instance not deployed on the machine
    num_of_no_capacity=0
    machine_deploy_dict={}
    machine_num=np.zeros((len(machine_resources),1))
    #Initial deployment
    for j in range(len(instance_info_data)):
        if (instance_info_data['machine_id'].iloc[j]>0):
            i=machine_number[instance_info_data['machine_id'].iloc[j]]
            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
            machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
            machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]
            if machine_deploy_dict.get(machine_instance_list[j,1])!=None:
                machine_deploy_dict.get(machine_instance_list[j,1]).append(machine_instance_list[j,0])
            else:
                machine_deploy_dict[machine_instance_list[j,1]]=[machine_instance_list[j,0]]
            machine_num[i]+=1
    #原来所属的机器删除旧实例的更新资源数据
    machine_resources_update=machine_resources.copy()
    
    
    
    point_of_instance=0
    machine_resources[0:mid_machine,0:T]+=machine_resources_data[cpu_columns].iloc[0:mid_machine]/machine_resources_data[cpu_columns].iloc[0]*(1-cpu_limits_1)/cpu_limits_1
    machine_resources[mid_machine:,0:T]+=machine_resources_data[cpu_columns].iloc[mid_machine:]/machine_resources_data[cpu_columns].iloc[0]*(1-cpu_limits_2)/cpu_limits_1
    
    deploy_machine=np.unique(machine_instance_list[:,1])
    point_of_machine=len(deploy_machine)
    init_machine_resources=init_machine_resources[0:point_of_machine,:]
    
    for move_round in range(1):       
        for j in range(point_of_instance,len(instance_info)):
            no_capacity=True
            machine_resources_update[machine_number[machine_instance_list[j,1]],:]+=instance_info[j,:]
            machine_num[machine_number[machine_instance_list[j,1]]]-=1
            reorder=np.argsort(mycov(machine_resources_update[0:point_of_machine,:],instance_info[j,:]))[::-1]
            for i in reorder[0:int(point_of_machine*more_machine(float(j)/len(instance_info)))] :
                # 判断服务器上资源是否足够
                if i==machine_number[machine_instance_list[j,1]]:
                    machine_resources_update[machine_number[machine_instance_list[j,1]],:]=machine_resources_update[machine_number[machine_instance_list[j,1]],:]-instance_info[j,:]
                    machine_num[machine_number[machine_instance_list[j,1]]]+=1
                    break
                if np.min(machine_resources[i,:]-instance_info[j,:])>=0 and np.min(machine_resources_update[i,:]-instance_info[j,:])>=0:
                    #app_interference
                    temp_list=machine_deploy_dict.get(machine_resources_data['machine_id'].iloc[i])
                    temp_app_id=instance_info_data['app_id'].iloc[j]
                    temp=True
                    #temp_list[k]:app id in machine ,temp_app_id:this app id
                    if temp_list!=None:
                        for k in range(len(temp_list)):
                            if app_interference_dict.get(temp_list[k]*app_max+temp_app_id)!=None:
                                if app_interference_dict.get(temp_list[k]*app_max+temp_app_id) \
                                    < temp_list.count(temp_app_id)+1-int(temp_list[k]==temp_app_id):
                                    temp=False
                                    #print('point_of_machine',point_of_machine,move_round)
                                    break
                            if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)!=None:
                                if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)\
                                    < temp_list.count(temp_list[k]):
                                    temp=False
                                    break
                    #reset machine resource
                    if temp:
                        machine_resources[i,:]-=instance_info[j,:]
                        machine_num[i]+=1
                        machine_resources_update[i,:]-=instance_info[j,:]
                        #machine_resources_update[machine_number[machine_instance_list[j,1]],:]=\
                        #machine_resources_update[machine_number[machine_instance_list[j,1]],:]+instance_info[j,:]
                        machine_instance_list_update[j,0]=instance_info_data['app_id'].iloc[j]
                        machine_instance_list_update[j,1]=machine_resources_data['machine_id'].iloc[i] 
                        if machine_deploy_dict.get(machine_instance_list_update[j,1])!=None:
                            machine_deploy_dict.get(machine_instance_list_update[j,1]).append(machine_instance_list_update[j,0])
                        else:
                            machine_deploy_dict[machine_instance_list_update[j,1]]=[machine_instance_list_update[j,0]]
                        if point_of_machine <=i:
                            point_of_machine=i
                        no_capacity=False
                        print (str(3)+',inst_'+str(instance_info_data['inst_id'].iloc[j])+',machine_'+\
                               str(machine_resources_data['machine_id'].iloc[i]),file=file_reulst)
                        break
            
            if no_capacity:
                num_of_no_capacity+=1
                print ('no capacity',instance_info_data['inst_id'].iloc[j])
            #if num_of_no_capacity>100:
            #    break
        machine_instance_list[machine_instance_list_update[:,0]>0]=machine_instance_list_update[machine_instance_list_update[:,0]>0]
    print ('num_of_no_capacity:',num_of_no_capacity)
    print ('machine number:',point_of_machine)
    file_reulst.close()


    
    end = time.time()
    print ("程序运行时间：",(end-start)/3600)
    

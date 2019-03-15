#coding=utf-8
import pandas as pd
import time
import numpy as np



def mycov(machine_resources,instance_info):
    machine_resources=machine_resources[:,0:98]
    instance_info=instance_info[0:98]
    result=np.dot(machine_resources,instance_info.T)
    return result

def more_machine(n):
    return 0.1+0.9*n*n


if __name__ == '__main__':
    #read data
    start = time.time()
    machine_resources_data = pd.read_csv('../data/my_machine_resources.csv')
    instance_info_data = pd.read_csv('../data/my_instance_info.csv')
    app_interference_data = pd.read_csv('../data/my_app_interference.csv')
    #file_name='../submit/'+'submit.csv'
    file_name='../submit/'+'submit_'+time.strftime("%Y%m%d_%H%M%S")+'.csv'
    file_reulst=open(file_name,'w')
    
    #参数
    T=98
    alpha=10
    beta=0.5
    cpu_limits_1=0.6
    cpu_limits_2=0.6
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
        
    #the instance takes more space first
    instance_info=instance_info_data[all_columns].values
    capacity=np.zeros((len(instance_info_data),1))
    for i in range(len(instance_info_data)):
        if np.min(machine_resources[-1,:]*Sorting_threshold-instance_info[i,:])<0:
            capacity[i]=1
            if np.min(machine_resources[-1,:]-instance_info[i,:])<0:
                capacity[i]=2
    print (len(instance_info_data[capacity==2]),len(instance_info_data\
           [capacity==1]),len(instance_info_data[capacity==0]))
    instance_info_data=pd.concat([instance_info_data[capacity==2],\
            instance_info_data[capacity==1],instance_info_data[capacity==0]])
    
    
    
    #dataframe -> np.array
    instance_info=instance_info_data[all_columns].values.astype(float)
    
    #convert to percent
    instance_info=instance_info/machine_resources[0,:]
    machine_resources=machine_resources/machine_resources[0,:]
    
    #instance deployment list
    machine_instance_list=np.zeros((len(instance_info),2))
    machine_instance_list_update=np.zeros((len(instance_info),2))
    
    #number of deployed machine
    point_of_machine=5000
    
    #number of instance not deployed on the machine
    num_of_no_capacity=0
    machine_deploy_dict={}
    #Initial deployment
    for j in range(len(instance_info_data)):
        if (instance_info_data['machine_id'].iloc[j]>0):
            i=np.nonzero(machine_resources_data['machine_id']==int(instance_info_data['machine_id'].iloc[j]))
            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
            machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
            machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]
            if machine_deploy_dict.get(machine_instance_list[j,1])!=None:
                machine_deploy_dict.get(machine_instance_list[j,1]).append(machine_instance_list[j,0])
            else:
                machine_deploy_dict[machine_instance_list[j,1]]=[machine_instance_list[j,0]]
    #原来所属的机器删除旧实例的更新资源数据
    machine_resources_update=machine_resources.copy()
    
    machine_number={}
    for i in range(len(machine_resources)):
        machine_number[machine_resources_data['machine_id'].iloc[i]]=i
    
    point_of_instance=0
    machine_resources[0:mid_machine,0:T]+=machine_resources_data[cpu_columns].iloc[0:mid_machine]/machine_resources_data[cpu_columns].iloc[0]*(1-cpu_limits_1)/cpu_limits_1
    machine_resources[mid_machine:,0:T]+=machine_resources_data[cpu_columns].iloc[mid_machine:]/machine_resources_data[cpu_columns].iloc[0]*(1-cpu_limits_2)/cpu_limits_1
    
    for move_round in range(2):       
        for j in range(point_of_instance,len(instance_info)):
            if point_of_machine>=6000 and move_round<1:
                #update machine resources and ond instance deployment
                machine_resources=machine_resources_update.copy()
                machine_resources[0:mid_machine,0:T]+=machine_resources_data[cpu_columns].iloc[0:mid_machine]/machine_resources_data[cpu_columns].iloc[0]*(1-cpu_limits_1)/cpu_limits_1
                machine_resources[mid_machine:,0:T]+=machine_resources_data[cpu_columns].iloc[mid_machine:]/machine_resources_data[cpu_columns].iloc[0]*(1-cpu_limits_2)/cpu_limits_1
                machine_instance_list[np.nonzero(machine_instance_list_update[:,1]>0),:]=\
                machine_instance_list_update[np.nonzero(machine_instance_list_update[:,1]>0),:]
                machine_instance_list_update[np.nonzero(machine_instance_list_update[:,1]>0),:]=0
                del machine_deploy_dict
                machine_deploy_dict={}
                for k in range(len(instance_info_data)):
                    if machine_deploy_dict.get(machine_instance_list[k,1])!=None:
                        machine_deploy_dict.get(machine_instance_list[k,1]).append(machine_instance_list[k,0])
                    else:
                        machine_deploy_dict[machine_instance_list[k,1]]=[machine_instance_list[k,0]]
                point_of_machine=5000
                point_of_instance=j
                break
            if machine_number[instance_info_data['machine_id'].iloc[j]]<point_of_machine and np.min(machine_resources_update[machine_number[instance_info_data['machine_id'].iloc[j]],0:T])>0:
                continue
            no_capacity=True
            reorder=np.argsort(mycov(machine_resources_update[0:point_of_machine,:],instance_info[j,:]))[::-1]
            for i in reorder[0:int(point_of_machine*more_machine(float(j)/len(instance_info)))] :
                # 判断服务器上资源是否足够
                if min(machine_resources[i,:]-instance_info[j,:])>=0 and min(machine_resources_update[i,:]-instance_info[j,:])>=0:
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
                                    print('point_of_machine',point_of_machine)
                                    break
                            if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)!=None:
                                if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)\
                                    < temp_list.count(temp_list[k]):
                                    temp=False
                                    break
                    #reset machine resource
                    if temp:
                        machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
                        machine_resources_update[i,:]=machine_resources_update[i,:]-instance_info[j,:]
                        machine_resources_update[machine_number[machine_instance_list[j,1]],:]=\
                        machine_resources_update[machine_number[machine_instance_list[j,1]],:]+instance_info[j,:]
                        machine_instance_list_update[j,0]=instance_info_data['app_id'].iloc[j]
                        machine_instance_list_update[j,1]=machine_resources_data['machine_id'].iloc[i] 
                        if machine_deploy_dict.get(machine_instance_list_update[j,1])!=None:
                            machine_deploy_dict.get(machine_instance_list_update[j,1]).append(machine_instance_list_update[j,0])
                        else:
                            machine_deploy_dict[machine_instance_list_update[j,1]]=[machine_instance_list_update[j,0]]
                        if point_of_machine <=i:
                            point_of_machine=i
                        no_capacity=False
                        print (str(move_round+1)+',inst_'+str(instance_info_data['inst_id'].iloc[j])+',machine_'+\
                               str(machine_resources_data['machine_id'].iloc[i]),file=file_reulst)
                        break
            if no_capacity:
                for i in range(point_of_machine,len(machine_resources)):
                    # 判断服务器上资源是否足够
                    if np.min(machine_resources[i,:]-instance_info[j,:])>=0 and min(machine_resources_update[i,:]-instance_info[j,:])>=0:
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
                                        break
                                if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)!=None:
                                    if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)\
                                        < temp_list.count(temp_list[k]):
                                        temp=False
                                        break
                        #reset machine resource
                        if temp:
                            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
                            machine_resources_update[i,:]=machine_resources_update[i,:]-instance_info[j,:]
                            machine_resources_update[machine_number[machine_instance_list[j,1]],:]=\
                            machine_resources_update[machine_number[machine_instance_list[j,1]],:]+instance_info[j,:]
                            machine_instance_list_update[j,0]=instance_info_data['app_id'].iloc[j]
                            machine_instance_list_update[j,1]=machine_resources_data['machine_id'].iloc[i] 
                            if machine_deploy_dict.get(machine_instance_list_update[j,1])!=None:
                                machine_deploy_dict.get(machine_instance_list_update[j,1]).append(machine_instance_list_update[j,0])
                            else:
                                machine_deploy_dict[machine_instance_list_update[j,1]]=[machine_instance_list_update[j,0]]
                            if point_of_machine <=i:
                                point_of_machine=i
                            no_capacity=False
                            print (str(move_round+1)+',inst_'+str(instance_info_data['inst_id'].iloc[j])+',machine_'+\
                                   str(machine_resources_data['machine_id'].iloc[i]),file=file_reulst)
                            break
            if no_capacity:
                num_of_no_capacity+=1
                print ('no capacity',instance_info_data['inst_id'].iloc[j])
    print ('num_of_no_capacity:',num_of_no_capacity)
    print ('machine number:',point_of_machine)
    file_reulst.close()


    
    end = time.time()
    print ("程序运行时间：",(end-start)/3600)
    
    #calculate score
    machine_resources_data = pd.read_csv('../data/my_machine_resources.csv')
    instance_info_data = pd.read_csv('../data/my_instance_info.csv')
    app_interference_data = pd.read_csv('../data/my_app_interference.csv')
    submit = pd.read_csv(file_name,header=None)
    
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
            
    if np.min(machine_resources)<0:
        print("空间不足")
    deploy_machine=np.unique(machine_instance_list[:,1])
    score=machine_resources[:,0:T]/machine_resources_data[cpu_columns].values.astype(float)
    score=1-score-beta
    score[score<0]=0
    alpha=machine_num+1
    print ("最后得分：",(np.sum(1+alpha*(np.exp(score)-1)))/98+len(deploy_machine)-len(machine_resources))
    machine_score=np.sum(1+alpha*(np.exp(score)-1),axis=1)/98

#coding=utf-8
import pandas as pd
import time
import numpy as np


def mycov(machine_resources,instance_info):
    machine_resources=machine_resources[:,0:98]
    instance_info=instance_info[0:98]
    #machine_resources=(machine_resources.T-np.mean(machine_resources,axis=1).T).T
    #instance_info=instance_info-np.mean(instance_info)
    result=np.dot(machine_resources,instance_info.T)
    return result

def more_machine(n):
    return n*1.1+0.1

def cal_score(score,i,j,cpu_limits_1,cpu_limits_2):
    if j<30000:
        return 1
    T=98
    alpha=10
    beta=0.5
    if i<3000:
        score=cpu_limits_1-cpu_limits_1*score-beta
    else:
        score=cpu_limits_2-cpu_limits_2*score-beta  
    score[score<0]=0
    return np.sum(1+alpha*(np.exp(score)-1))/T

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
    cpu_limits_1=0.5
    cpu_limits_2=0.5
    Sorting_threshold=0.25
    max_score=1.5
    #more_machine=0.1
    
    cpu_columns=['cpu'+str(i) for i in range(T)]
    mem_columns=['mem'+str(i) for i in range(T)]
    all_columns=cpu_columns+mem_columns+['disk','P','M']
    
    #Reverse order of machine resources
    machine_resources_data=machine_resources_data.iloc[np.argsort(machine_resources_data['disk'].values)[::-1]]
    machine_resources=machine_resources_data[all_columns].values.astype(float)
    machine_resources[0:3000,0:T]=machine_resources[0:3000,0:T]*cpu_limits_1
    machine_resources[3000:,0:T]=machine_resources[3000:,0:T]*cpu_limits_2
    
    #app interference dict
    app_max=instance_info_data['app_id'].max()
    app_interference_dict={}
    app_interference=app_interference_data.values
    for i in range(len(app_interference)):
        app_interference_dict[app_interference[i,0]*app_max+app_interference[i,1]]=app_interference[i,2]
        
    #the instance takes more space first
    instance_info=instance_info_data[all_columns].values
    capacity=np.zeros((len(instance_info_data),1))
    for i in range(len(instance_info_data)):
        if np.min(machine_resources[-1,:]*Sorting_threshold-instance_info[i,:])<0:
            capacity[i]=1
            if np.min(machine_resources[-1,:]-instance_info[i,:])<0:
                capacity[i]=2
    print (len(instance_info_data[capacity==2]),len(instance_info_data[capacity==1]),len(instance_info_data[capacity==0]))
    instance_info_data=pd.concat([instance_info_data[capacity==2],\
                                  instance_info_data[capacity==1],instance_info_data[capacity==0]])
    
    
    
    #dataframe -> np.array
    instance_info=instance_info_data[all_columns].values.astype(float)
    
    #convert to percent
    #instance_info=instance_info/machine_resources[0,:]
    #machine_resources=machine_resources/machine_resources[0,:]
    init_machine_resources=machine_resources_data[all_columns].values.astype(float)
    init_machine_resources[0:3000,0:T]=init_machine_resources[0:3000,0:T]*cpu_limits_1
    init_machine_resources[3000:,0:T]=init_machine_resources[3000:,0:T]*cpu_limits_2
    
    #instance deployment list
    machine_instance_list=np.zeros((len(instance_info),2))
    
    #number of deployed machine
    point_of_machine=0
    
    #number of instance not deployed on the machine
    num_of_no_capacity=0
    
        
    #Initial deployment
    for j in range(len(instance_info_data)):
        if (instance_info_data['machine_id'].iloc[j]>0):
            i=np.nonzero(machine_resources_data['machine_id']==int(instance_info_data['machine_id'].iloc[j]))
            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
            machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
            machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]

    for j in range(len(instance_info)):
        if (instance_info_data['machine_id'].iloc[j]>0):
            no_capacity=True
            reorder=np.argsort(mycov(machine_resources[0:point_of_machine,:],instance_info[j,:]))[::-1]
            for i in reorder[0:int(point_of_machine*more_machine(float(j)/len(instance_info)))] :
                # 判断服务器上资源是否足够
                #print( cal_score(machine_resources[i,0:T]/init_machine_resources[i,0:T],i,cpu_limits_1,cpu_limits_2))
                if min(machine_resources[i,:]-instance_info[j,:])>=0 and cal_score(machine_resources[i,0:T]/init_machine_resources[i,0:T],i,j,cpu_limits_1,cpu_limits_2)<max_score:
                    #app_interference
                    temp_list=(machine_instance_list[:,0][machine_instance_list[:,1]==\
                                                     machine_resources_data['machine_id'].iloc[i]])
                    temp_app_id=instance_info_data['app_id'].iloc[j]
                    temp=True
                    #temp_list[k]:app id in machine ,temp_app_id:this app id
                    for k in range(len(temp_list)):
                            if app_interference_dict.get(temp_list[k]*app_max+temp_app_id)!=None:
                                if app_interference_dict.get(temp_list[k]*app_max+temp_app_id) \
                                    < len(temp_list[temp_list==temp_app_id])+1-int(temp_list[k]==temp_app_id):
                                    temp=False
                                    break
                            if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)!=None:
                                if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)\
                                    < len(temp_list[temp_list==temp_list[k]]):
                                    temp=False
                                    print('point_of_machine',point_of_machine)
                                    break
                    #reset machine resource
                    if temp:
                        machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
                        machine_resources[np.nonzero(machine_resources_data['machine_id']==int(instance_info_data['machine_id'].iloc[j])),:]=\
                        machine_resources[np.nonzero(machine_resources_data['machine_id']==int(instance_info_data['machine_id'].iloc[j])),:]+instance_info[j,:]
                        machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
                        machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]   
                        if point_of_machine <i:
                            point_of_machine=i
                        no_capacity=False
                        print ('inst_'+str(instance_info_data['inst_id'].iloc[j])+',machine_'+\
                               str(machine_resources_data['machine_id'].iloc[i]),file=file_reulst)
                        break
            if no_capacity:
                for i in range(point_of_machine,len(machine_resources)):
                    # 判断服务器上资源是否足够
                    if np.min(machine_resources[i,:]-instance_info[j,:])>=0:
                        #app_interference
                        temp_list=(machine_instance_list[:,0][machine_instance_list[:,1]==\
                                                         machine_resources_data['machine_id'].iloc[i]])
                        temp_app_id=instance_info_data['app_id'].iloc[j]
                        temp=True
                        #temp_list[k]:app id in machine ,temp_app_id:this app id
                        for k in range(len(temp_list)):
                            if app_interference_dict.get(temp_list[k]*app_max+temp_app_id)!=None:
                                if app_interference_dict.get(temp_list[k]*app_max+temp_app_id) \
                                    < len(temp_list[temp_list==temp_app_id])+1-int(temp_list[k]==temp_app_id):
                                    temp=False
                                    break
                            if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)!=None:
                                if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)\
                                    < len(temp_list[temp_list==temp_list[k]]):
                                    temp=False
                                    break
                        #reset machine resource
                        if temp:
                            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
                            machine_resources[np.nonzero(machine_resources_data['machine_id']==int(instance_info_data['machine_id'].iloc[j])),:]=\
                            machine_resources[np.nonzero(machine_resources_data['machine_id']==int(instance_info_data['machine_id'].iloc[j])),:]+instance_info[j,:]
                            machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
                            machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]   
                            if point_of_machine <i:
                                point_of_machine=i
                            no_capacity=False
                            print ('inst_'+str(instance_info_data['inst_id'].iloc[j])+',machine_'+\
                                   str(machine_resources_data['machine_id'].iloc[i]),file=file_reulst)
                            break
            if no_capacity:
                num_of_no_capacity+=1
                print ('no capacity',instance_info_data['inst_id'].iloc[j])
            
        else:
            no_capacity=True
            reorder=np.argsort(mycov(machine_resources[0:point_of_machine,:],instance_info[j,:]))[::-1]
            for i in reorder[0:int(point_of_machine*more_machine(float(j)/len(instance_info)))]:
                #search machine
                # 判断服务器上资源是否足够
                if np.min(machine_resources[i,:]-instance_info[j,:])>=0 and cal_score(machine_resources[i,0:T]/init_machine_resources[i,0:T],i,j,cpu_limits_1,cpu_limits_2)<max_score:
                    #app_interference
                    temp_list=(machine_instance_list[:,0][machine_instance_list[:,1]==\
                                                     machine_resources_data['machine_id'].iloc[i]])
                    temp_app_id=instance_info_data['app_id'].iloc[j]
                    temp=True
                    #temp_list[k]:app id in machine ,temp_app_id:this app id
                    for k in range(len(temp_list)):
                            if app_interference_dict.get(temp_list[k]*app_max+temp_app_id)!=None:
                                if app_interference_dict.get(temp_list[k]*app_max+temp_app_id) \
                                    < len(temp_list[temp_list==temp_app_id])+1-int(temp_list[k]==temp_app_id):
                                    temp=False
                                    break
                            if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)!=None:
                                if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)\
                                    < len(temp_list[temp_list==temp_list[k]]):
                                    temp=False
                                    break
                    #reset machine resource
                    if temp:
                        machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
                        machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
                        machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]   
                        if point_of_machine <i:
                            point_of_machine=i
                        no_capacity=False
                        print ('inst_'+str(instance_info_data['inst_id'].iloc[j])+',machine_'+\
                               str(machine_resources_data['machine_id'].iloc[i]),file=file_reulst)
                        break
            if no_capacity:
                for i in range(point_of_machine,len(machine_resources)):
                    #search machine
                    # 判断服务器上资源是否足够
                    if np.min(machine_resources[i,:]-instance_info[j,:])>=0:
                        #app_interference
                        temp_list=(machine_instance_list[:,0][machine_instance_list[:,1]==\
                                                         machine_resources_data['machine_id'].iloc[i]])
                        temp_app_id=instance_info_data['app_id'].iloc[j]
                        temp=True
                        #temp_list[k]:app id in machine ,temp_app_id:this app id
                        for k in range(len(temp_list)):
                            if app_interference_dict.get(temp_list[k]*app_max+temp_app_id)!=None:
                                if app_interference_dict.get(temp_list[k]*app_max+temp_app_id) \
                                    < len(temp_list[temp_list==temp_app_id])+1-int(temp_list[k]==temp_app_id):
                                    temp=False
                                    break
                            if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)!=None:
                                if app_interference_dict.get(temp_list[k]+temp_app_id*app_max)\
                                    < len(temp_list[temp_list==temp_list[k]]):
                                    temp=False
                                    break
                        #reset machine resource
                        if temp:
                            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
                            machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
                            machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]   
                            if point_of_machine <i:
                                point_of_machine=i
                            no_capacity=False
                            print ('inst_'+str(instance_info_data['inst_id'].iloc[j])+',machine_'+\
                                   str(machine_resources_data['machine_id'].iloc[i]),file=file_reulst)
                            break
            if no_capacity:
                num_of_no_capacity+=1
                print ('no capacity')
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
   
    submit[0]=submit[0].str.split('_',expand=True)[1].astype(int)
    submit[1]=submit[1].str.split('_',expand=True)[1].astype(int)
    submit.columns=['inst_id','new_machine_id']
    submit=pd.merge(instance_info_data[['inst_id','machine_id']],submit,on='inst_id',how='left')
    instance_info_data['machine_id'][submit['new_machine_id']>0]=submit['new_machine_id'][submit['new_machine_id']>0]
    machine_resources=machine_resources_data[all_columns].values.astype(float)
    instance_info=instance_info_data[all_columns].values
    machine_instance_list=np.zeros((len(instance_info),2))


    for j in range(len(instance_info_data)):
        if (instance_info_data['machine_id'].iloc[j]>0):
            i=np.nonzero(machine_resources_data['machine_id']==int(instance_info_data['machine_id'].iloc[j]))
            machine_resources[i,:]=machine_resources[i,:]-instance_info[j,:]
            machine_instance_list[j,0]=instance_info_data['app_id'].iloc[j]
            machine_instance_list[j,1]=machine_resources_data['machine_id'].iloc[i]
            
    
    deploy_machine=np.unique(machine_instance_list[:,1])
    score=machine_resources[:,0:T]/machine_resources_data[cpu_columns].values.astype(float)
    score=1-score-beta
    score[score<0]=0
    print ("最后得分：",(np.sum(1+alpha*(np.exp(score)-1)))/98+len(deploy_machine)-len(machine_resources))
    machine_score=np.sum(1+alpha*(np.exp(score)-1),axis=1)/98

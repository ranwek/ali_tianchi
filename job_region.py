#coding=utf-8

def update(key,update_max):
    global job_dict,job_max,job_min
    job_time=int(job_dict.get(key)[4])
    if job_max[key]>=update_max:
        job_max[key]=update_max
        for predecessor_key in job_dict[key][5:]:
            if job_dict.get(predecessor_key)!=None:
                # deploy predecessor
                update(predecessor_key,job_max[key]-int(1.16*job_time))
                job_min[key]=job_max[key]-int(1.16*job_time)
        
def update1(key,update_max):
    global job_dict,job_max,job_min
    job_time=int(job_dict.get(key)[4])
    if job_max[key]>=update_max:
        job_max[key]=update_max
        for predecessor_key in job_dict[key][5:]:
            if job_dict.get(predecessor_key)!=None:
                # deploy predecessor
                update(predecessor_key,job_max[key]-int(300+job_time))
                job_min[key]=job_max[key]-int(300+job_time)

def update2(key,update_max):
    global job_dict,job_max,job_min
    job_time=int(job_dict.get(key)[4])
    if job_max[key]>=update_max:
        job_max[key]=update_max
        for predecessor_key in job_dict[key][5:]:
            if job_dict.get(predecessor_key)!=None:
                # deploy predecessor
                update(predecessor_key,job_max[key]-int(600+job_time))
                job_min[key]=job_max[key]-int(600+job_time)
                
if __name__ == '__main__':    
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
    
    job_max={}
    job_min={}
    for each_job_key in job_dict.keys():
        #左闭右开
        job_max[each_job_key]=1470
        job_min[each_job_key]=0
    
    for each_job_key in job_dict.keys():
        if job_dict.get(job_dict[each_job_key][5])!=None:
            #deploy predecessor
            if int(each_job_key.split('-')[1])>20:
                update(each_job_key,1470)
            elif int(each_job_key.split('-')[1])>6:
                update1(each_job_key,1470)
            else:
                update2(each_job_key,1470)


    for each_job_key in job_dict.keys():
        if job_min[each_job_key]<0 or job_min[each_job_key]>job_max[each_job_key]:
            print (job_min[each_job_key],each_job_key)
        if job_max[each_job_key]>1470:
            print (job_max[each_job_key],each_job_key)
        if job_max[each_job_key]-job_min[each_job_key]<int(job_dict.get(each_job_key)[4]):
            print(job_max[each_job_key],job_min[each_job_key],int(job_dict.get(each_job_key)[4]),each_job_key)
        for predecessor_key in job_dict[each_job_key][5:]:
                if job_dict.get(predecessor_key)!=None:
                    if job_max[predecessor_key]>job_min[each_job_key]:
                        print(job_max[predecessor_key],job_min[each_job_key],each_job_key)
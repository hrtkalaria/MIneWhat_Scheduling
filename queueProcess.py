# -*- coding: utf-8 -*-
"""
Created on Wed Mar 08 17:08:31 2017

@author: Hrt
"""
import pandas as pd
import time
import sys


arg = sys.argv
#print arg
file_path = arg[1]
system_time = arg[2]
#print system_time
sys_date=system_time.split(' ')[0]

past_date = sys_date.split('/')[2]
sys_time=system_time.split(' ')[1]
current_time_hour=int(sys_time.split(':')[0]) *60
current_time_minute=int(sys_time.split(':')[1])
past=current_time_hour+current_time_minute
delay=0
time_delay =0
date_delay=0
#print sys_date, sys_time

df=pd.read_csv(file_path,header=0)
#print df
#df['time_to_expire'] = df['time_to_expire'].apply(lambda x: x.split(' '))
df_time_date=df['time_to_expire'].str[0:].str.split(' ', expand=True)
#print df_time_date
df_time_date.columns =['date_to_expire','time_to_expire']
#print df_time_date
del df['time_to_expire']
df=pd.concat([df,df_time_date], axis=1)
#print df
df=df.sort_values(['date_to_expire', 'time_to_expire'], ascending=[True,True])
#print df

df=df[(df.date_to_expire >= sys_date) & (df.time_to_expire >= sys_time)]
#print df
#=======================================================================
class Queue:
    def __init__(self):
        self.queue_list=[]
    def enqueue(self,obj):
        self.queue_list.append(obj)
    def dequeue(self):
        return self.queue_list.pop(0)
#=======================================================================        
def schedule(y):
    df_process= df.loc[y[1]]
    len_record=len(df_process)
    #print len_record
    df_process['priority'].fillna(len_record+1,inplace=True)
    df_sorted_priority=df_process.sort('priority')
    
    i=0
    while i!= len_record:
        print 'Current time ['+df_sorted_priority.iloc[i]['date_to_expire']+' '+df_sorted_priority.iloc[i]['time_to_expire']+'] Event '+df_sorted_priority.iloc[i]['event_name'] +' Processed'
        i+=1
#=======================================================================      
def scheduling_logic(x):
    global past
    global delay
    global time_delay
    global date_delay
    df_time=df.loc[x]
    gp_time = df_time.groupby('time_to_expire')
    count_time = len(gp_time.groups.items())
    group_time = gp_time.groups.items()
    #print group_time[0][0]
    xyz = sorted(group_time, key=lambda s: s[:][0])
    i=0
    
    while i!=count_time:
        #print xyz[i][0]
        hr=xyz[i][0].split(':')
        #print hr
        hour=60*int(hr[0])
        minute=int(hr[1])
        current=hour+minute
        time_delay=current-past
        delay = time_delay+date_delay
        past = current
        #print delay
        time.sleep(delay*60)
        schedule(xyz[i])
        i+=1
        
        
#========================================================================
queue = Queue()

gp_date = df.groupby('date_to_expire')
count_date = len(gp_date.groups.items())
group_date = gp_date.groups.items()
sorted_group_date = sorted(group_date, key=lambda s: s[:][0])
j=0
while j!=count_date:
    dt_split = sorted_group_date[j][0].split('/')
    date_diff = int(dt_split[2]) - int(past_date)
    date_delay = date_diff*24*60
    scheduling_logic(sorted_group_date[j][1])
    past_date= dt_split[2]
    j+=1




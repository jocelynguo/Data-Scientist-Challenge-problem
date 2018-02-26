# created by Jia Guo
# title: Asana data scientist intern challenge
# date: 02/25/2018

import pandas as pd
import numpy as np
from datetime import timedelta


# import user_engagement by pandas dataframe
df = pd.read_csv('takehome_user_engagement.csv')



# get the total log-in frequencies of each user regardless of login time
total_login = df.groupby('user_id').size().reset_index(name='total_login_freqencies') 
total_login.head()


# drop the rows which the total log-in freqencies are less than 3
total_login = total_login[total_login.total_login_freqencies >= 3]
total_login.head()


# create a merged_inner dataframe
# to start analyze the time_stamp in each 7-day period to keep eliminating users
merged_inner = pd.merge(left=df,right=total_login, left_on='user_id', right_on='user_id')
merged_inner.head()


# delete the time part in the time_stamp
merged_inner['time_stamp'] = pd.to_datetime(merged_inner['time_stamp'], errors='coerce')
merged_inner['time_stamp'] = merged_inner['time_stamp'].dt.date
dfnew = merged_inner.drop(['visited', 'total_login_freqencies'], 1)
dfnew.head()


# check time of each column make sure error-free to calculate the log-in gap-day of each user
dfnew['time_stamp'] = pd.to_datetime(dfnew['time_stamp'])
dfnew.dtypes


# calculate the gap during each 3 times of login for each user
dfnew['lastlast_login'] = dfnew.groupby('user_id')['time_stamp'].shift(2)
dfnew['time_diff'] = dfnew['time_stamp'] - dfnew['lastlast_login']
dfnew.head()


# remove all rows with NaT value in time_diff column
dfnew = dfnew[dfnew.time_diff.notnull()]                  
dfnew.head()


# convert the time_diff components to integer in days
dfnew['time_diff'] = (dfnew.time_diff / np.timedelta64(1, 'D')).astype(int)
print(type(dfnew['time_diff']))
dfnew.head()


# In[11]:


# the user_id showed in below output dataframe should be the adopted user
dfnew = dfnew.drop(dfnew[dfnew['time_diff']>7].index)
dfnew = dfnew.drop_duplicates(subset=['user_id'])
dfnew.head()


# get the current existing adopted user list by user id
adopted_user_list = []
adopted_user_list = dfnew['user_id'].values
num_adopted_user = len(adopted_user_list)
print('number of current existing adopted user are:', num_adopted_user)
print('and their user ids are:', adopted_user_list)


# read in the takehome_users.csv
df1 = pd.read_csv('takehome_users.csv', encoding='cp1252')
df1.head()


# In[14]:


# takes only some specific columns
df1 = df1.drop(['creation_time', 'name', 'email' ], axis=1)
df1.head()


# convert the unix timestamp to readable datetime
df1['last_session_creation_time'] = (pd.to_datetime(df1['last_session_creation_time'],unit='s')) 


# merge two dataframe to discover the hidden pattern in the current exiting adopted users
dfnew1 = pd.merge(left=df1,right=dfnew, left_on='object_id', right_on='user_id')
dfnew1 = dfnew1.drop(['time_stamp', 'user_id', 'lastlast_login', 'time_diff', ], axis=1)
dfnew1.head(10)


# analyzing the email domain
temp1 = dfnew1.groupby(['email_domain']).size().reset_index(name='count')
temp1.sort_values('count', inplace=True)
temp1['email_domain_percent'] = temp1['count']*100/num_adopted_user
temp1.sort_values('email_domain_percent', ascending=False, inplace=True)
print(temp1.head(10))


# analyzing the creation source
temp = dfnew1.groupby(['creation_source']).size().reset_index(name='count')
temp['source_percent'] = temp['count']*100/num_adopted_user
temp.sort_values('source_percent', ascending=False, inplace=True)
print(temp)


# In[19]:


# calculate the percentage of opted_in_to_mailing_list of all existing adopted users
total = sum(df1['opted_in_to_mailing_list'])
percent_optin_mailinglist = total/num_adopted_user
print ('About', int(percent_optin_mailinglist),'percent of the current adopted users choose to opt-in the mailing list')


# calculate the percentage of enabled_for_marketing_drip of all existing adopted users
total1 = sum(df1['enabled_for_marketing_drip'])
percent_enable_marketing_drip = total1/num_adopted_user
print ('About', int(percent_enable_marketing_drip),'percent of the current adopted users choose to opt-in the mailing list')


# filter out the top 10 organization ID
temp2 = dfnew1
temp2 = temp2[temp2.creation_source.str.contains("ORG_INVITE") == True]
temp2 = temp2.groupby(['org_id']).size().reset_index(name='count')
temp2.sort_values('count', inplace=True, ascending=False)
temp2.head(10)


# filter out the top 10 existing users who invited others to use the product
temp3 = dfnew1
temp3 = temp3[temp3.creation_source.str.contains("GUEST_INVITE") == True]
temp3 = temp3.groupby(['invited_by_user_id']).size().reset_index(name='count')
temp3.sort_values('count', inplace=True, ascending=False)
temp3.head(10)


# get the detail count of last session creation time by specific date
temp4 = dfnew1
temp4['last_session_creation_time'] = temp4['last_session_creation_time'].dt.date
temp4 = temp4.groupby(['last_session_creation_time']).size().reset_index(name='count')
temp4.sort_values('count', inplace=True, ascending=False)
temp4


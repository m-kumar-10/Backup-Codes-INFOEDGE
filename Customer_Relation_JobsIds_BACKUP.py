import pymongo
import datetime
import pandas as pd
import datetime
import glob
import os
import sys
import re
import sqlalchemy
import numpy as np
from sendmail import send_mail
import pdb
import gzip

print "scheduled script is at -/home/analytics/scheduledCodes/Naukri/Recruiter_side_reports/Jobs_Dump_Cutomer_Relation/RCode/Customer_Relation_JobsIds.py"


def login(IP_PORT_DB):
    engine = sqlalchemy.create_engine(
        'mysql+pymysql://user_analytics:anaKm7Iv80l@172.10.112.'+str(IP_PORT_DB))
    return engine


def timeplot(dfr, col):
    dfr['Quarter'] = pd.PeriodIndex(dfr[col], freq='Q-Mar').strftime('Q%q')
    dfr['FY'] = pd.PeriodIndex(dfr[col], freq='A-Mar')
    dfr['FY'] = dfr['FY'].apply(lambda y: str(y-1)+"-"+str(y-2000))
    dfr['MONTH'] = dfr[col].dt.strftime('%b-%y')
    return dfr


def filter_action(dfr):
    dfr = dfr[(dfr['ACTION'].isin(['new', 'XML-new', 'qc-makelive'])) & (dfr['company_id'].isin(topcids))
              & ~(dfr['file'].isin(private_dup_jobs['file']))].reset_index(drop=True)
    return dfr
# to break a list in chunks and iterate over it


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


def MongoConnectRN(table, database='autosuggest_vertical',
                   username='Analytics_vertical',
                   password='V3rTic@lKm7Iv80l',
                   replica_host_port="mongodb://10.10.112.61:27017,10.10.112.62:27017,10.10.112.63:27017",
                   replicaset="RPL-SUGGESTION",
                   readPreference='secondary'

                   ):
    connection = pymongo.MongoClient(
        replica_host_port, replicaset=replicaset, readPreference=readPreference)
    authentication_status = connection[database].authenticate(
        username, password)
    table = connection[database][table]
    return table
# jobs_mongo_conn.find_one()


pd.set_option('max_columns', None)

mongo_ore_jobs_db = {
    "username": "analytics",
    "database": "jobs",
    "password": "analyticsmmonasses",
    "replica_host_port": "mongodb://oremongo1.resdex.com:27017,oremongo2.resdex.com:27017,oremongo3.resdex.com:27017",
    "replicaset": "job"
}

jobs_mongo_conn = MongoConnectRN("naukri_jobs", database=mongo_ore_jobs_db['database'], username=mongo_ore_jobs_db['username'], password=mongo_ore_jobs_db[
                                 'password'], replica_host_port=mongo_ore_jobs_db['replica_host_port'], replicaset='RPL-JOB', readPreference='secondary')


my_cond_live_jobs = {
    'deleted': False
}

fields_to_fetch = {
    '_id': 1,
    "createdDate": 1,
    'enrichedJobData.pmode': 1,
    'category': 1,
    'title': 1,
    'mode': 1,
    'description': 1,
    'indexerData.keywords': 1,

    'companyId': 1,
    "companyDetail.name": 1,
    "companyDetail.details": 1,
    "companyDetail.websiteUrl": 1,
    "companyDetail.address": 1,
    'registrationType': 1,

    'salaryDetail.maximumSalary': 1,
    'salaryDetail.minimumSalary': 1,
    'salaryDetail.currency': 1,
    'salaryDetail.hideSalary': 1,
    'salaryDetail.variablePercentage': 1,

    'maximumExperience': 1,
    'minimumExperience': 1,

    'enrichedJobData.city': 1,
    'industry': 1,
    'functionalArea': 1,
    'jobRole': 1,
    'localities': 1,

    'industryGid': 1,
    #     'localitiesGid':1,
    #     'nationalLocationsGid':1,
    #     'internationalLocationsGid':1,
    'functionalAreaGid': 1,
    'jobRoleGid': 1,

    'board': 1,
    'responseManager': 1,
    'responseManagerSettings.companyUrl': 1,
    'referenceCode': 1,
    'indexerData.jobsearchProcessed': 1,
    'indexerData.ugCourse': 1,
    'indexerData.pgCourse': 1,
    'education': 1
}
# 'education':1,

#  }


jobs_fetched = jobs_mongo_conn.find(my_cond_live_jobs, fields_to_fetch)
df = pd.DataFrame(list(jobs_fetched))

# #reading sample df of live jobs
# #split pandas dataframe in multiple dataframes, split csv files, divide csv files
# import numpy as np
# li=np.array_split(df,10)
# for i in range(len(li)):
#     li[i].to_pickle('li_{}.pkl'.format(i))

# k=glob.glob('/data/analytics/scheduledCodes/Naukri/Recruiter_side_reports/Jobs_Dump_Cutomer_Relation/RCode/li*.pkl')
#
# df=pd.concat([pd.read_pickle(x) for x in k])


def isPremiumJob(dseries_education):
    return dseries_education.apply(lambda di: sum([di[i] for i in di if i in ['mbaPremium', 'bTechPremium', 'obTechPremium', 'ombaPremium']]) if isinstance(di, dict) else 0)


df['COMNAME'] = df['companyDetail'].apply(
    lambda x: x.get('name') if not isinstance(x, float) else None)
df['companyDetail_details'] = df['companyDetail'].apply(
    lambda x: x.get('details') if not isinstance(x, float) else None)
df['companyDetail_websiteUrl'] = df['companyDetail'].apply(
    lambda x: x.get('websiteUrl') if not isinstance(x, float) else None)
df['companyDetail_address'] = df['companyDetail'].apply(
    lambda x: x.get('address') if not isinstance(x, float) else None)

df['Min_Salary'] = df['salaryDetail'].apply(lambda x: x.get(
    'minimumSalary') if not isinstance(x, float) else None)
df['Max_Salary'] = df['salaryDetail'].apply(lambda x: x.get(
    'maximumSalary') if not isinstance(x, float) else None)
df['Currency'] = df['salaryDetail'].apply(lambda x: x.get(
    'currency') if not isinstance(x, float) else None)
df['hideSalary'] = df['salaryDetail'].apply(lambda x: x.get(
    'hideSalary') if not isinstance(x, float) else None)
df['Salary_variablePercentage'] = df['salaryDetail'].apply(
    lambda x: x.get('variablePercentage') if not isinstance(x, float) else None)

df['CITY'] = df['enrichedJobData'].apply(
    lambda x: x.get('city') if not isinstance(x, float) else None)
df['UGCOURSE'] = df['indexerData'].apply(lambda x: x.get(
    'ugCourse') if not isinstance(x, float) else None)
df['PGCOURSE'] = df['indexerData'].apply(lambda x: x.get(
    'pgCourse', False) if not isinstance(x, float) else False)

df.to_pickle(
    '/data/analytics/scheduledCodes/Naukri/Recruiter_side_reports/Jobs_Dump_Cutomer_Relation/RCode/df_new.pkl')

# df['localitiesGid']=df['localitiesGid'].apply(lambda x:x.get('localitiesGid',False) if not isinstance(x,float) else False)
# df['nationalLocationsGid']=df['nationalLocationsGid'].apply(lambda x:x.get('nationalLocationsGid',False) if not isinstance(x,float) else False)
# df['internationalLocationsGid']=df['internationalLocationsGid'].apply(lambda x:x.get('internationalLocationsGid',False) if not isinstance(x,float) else False)

df['companyUrl'] = df['responseManagerSettings'].apply(
    lambda x: x.get('companyUrl') if not isinstance(x, float) else None)
df['keywords'] = df['indexerData'].apply(lambda x: x.get(
    'keywords') if not isinstance(x, float) else None)

df['Premium'] = isPremiumJob(df['education'])
df.loc[df['Premium'] >= 1, 'Premium'] = 1

df['Paf_Engg_Flag'] = df['education'].apply(lambda x: x.get(
    'bTechPremium', False) if not isinstance(x, float) else False)
df['Paf_Mba_Flag'] = df['education'].apply(lambda x: x.get(
    'mbaPremium', False) if not isinstance(x, float) else False)
df['Prem_Eng_Organic'] = df['education'].apply(lambda x: x.get(
    'obTechPremium', False) if not isinstance(x, float) else False)
df['Prem_MBA_Organic'] = df['education'].apply(lambda x: x.get(
    'ombaPremium', False) if not isinstance(x, float) else False)

df['pmode'] = df['enrichedJobData'].apply(
    lambda x: x.get('pmode') if not isinstance(x, float) else None)

df = df.rename(columns={'_id': 'file', 'createdDate': 'ADDATE', 'companyId': 'company_id', 'minimumExperience': 'MINEXP', 'maximumExperience': 'MAXEXP',
               'category': 'Job_Type', 'responseManager': 'Response_Mode', 'industry': 'indtype', 'functionalArea': 'farea_id', 'jobRole': 'ROLE'})

df['Min_Salary'] = pd.to_numeric(df['Min_Salary'], errors='coerce')
df['Max_Salary'] = pd.to_numeric(df['Max_Salary'], errors='coerce')
df['MINEXP'] = pd.to_numeric(df['MINEXP'], errors='coerce')
df['MAXEXP'] = pd.to_numeric(df['MAXEXP'], errors='coerce')

#role=pd.read_sql_query("SELECT PID AS FAREA,VAL AS ROLE, a.OPT_VAL, m.LABEL FROM FAREA_ROLE_MASTER m , FAREA_ROLE_APP_SORTING a WHERE m.MID = a.MID and a.APP_ID = 4 ORDER BY PID",login('62:3311/dd_naukri'))
#farea=pd.read_sql_query("SELECT MID,LABEL FROM FAREA_MASTER WHERE MID IN (select MID from FAREA_APP_SORTING where APP_ID=4) ORDER by MID;",login('62:3311/dd_naukri')).append(pd.DataFrame({'MID':[24],'LABEL':['IT Software']})).set_index('MID')
#indtype=pd.read_sql_query("SELECT MID,LABEL FROM INDUSTRY_TYPE_MASTER WHERE MID IN (select MID from INDUSTRY_TYPE_APP_SORTING where APP_ID=4) ORDER by MID;",login('62:3311/dd_naukri')).set_index('MID')


#role = pd.read_csv("FAREA_ROLE_MASTER ----FAREA_ROLE_APP_SORTING.csv")
#farea = pd.read_csv("FAREA_MASTER--FAREA_APP_SORTING.csv").append(pd.DataFrame({'MID':[24],'LABEL':['IT Software']})).set_index('MID')
#indtype= pd.read_csv("INDUSTRY_TYPE_MASTER--INDUSTRY_TYPE_APP_SORTING.csv").set_index('MID')
role = pd.read_csv(
    "/data/analytics/scheduledCodes/Naukri/Recruiter_side_reports/Jobs_Dump_Cutomer_Relation/RCode/FAREA_ROLE_MASTER ----FAREA_ROLE_APP_SORTING.csv")
farea = pd.read_csv("/data/analytics/scheduledCodes/Naukri/Recruiter_side_reports/Jobs_Dump_Cutomer_Relation/RCode/FAREA_MASTER--FAREA_APP_SORTING.csv").append(
    pd.DataFrame({'MID': [24], 'LABEL': ['IT Software']})).set_index('MID')

indtype = pd.read_csv(
    "/data/analytics/scheduledCodes/Naukri/Recruiter_side_reports/Jobs_Dump_Cutomer_Relation/RCode/INDUSTRY_TYPE_MASTER--INDUSTRY_TYPE_APP_SORTING.csv")


df['role_mapped'] = df['ROLE'].map(role.set_index('OPT_VAL')['LABEL'])

df['FAREA'] = pd.to_numeric(df['farea_id'], errors='coerce').fillna(
    0).astype(int).map(farea['LABEL'])
df['industry'] = pd.to_numeric(df['indtype'], errors='coerce').fillna(
    0).astype(int).map(indtype['LABEL'])

df['IsDuplicate'] = 'No'
df['IsPrivate'] = 'No'
df['IsCSite'] = 'No'
df['Is_Consultant'] = 'n'
df.loc[df['pmode'] == 'd', 'IsDuplicate'] = 'Yes'
df.loc[df['Job_Type'] == 'p', 'IsPrivate'] = 'Yes'
df.loc[df['board'] == '2', 'IsCSite'] = 'Yes'
df.loc[df['registrationType'] == 'consultant', 'Is_Consultant'] = 'y'


# keep in mind that indexerData is created by job search team. you can refer to education field as well
# for Mapping, confirm with Jaswinder/Jatin Jain from tech what to use? continue using JOBPOSTING data or dd_naukri data

# UG mapping
ug = pd.read_sql_query(
    "SELECT CAST(VAL AS CHAR) AS UGCOURSE ,COURSE as UGCOURSE_mapping from UG_COURSE", login('63:3306/JOBPOSTING'))
ug_map = ug.set_index('UGCOURSE').to_dict()['UGCOURSE_mapping']


def ug_mapper(s):
    if s is None:
        return ''
    else:
        return ','.join([ug_map[x] if x in ug_map else str(x) for x in s.split(',')])


df['UGCOURSE_mapped'] = df['UGCOURSE'].apply(lambda s: ug_mapper(s))

# PG mapping
# pg=pd.read_sql_query("SELECT CAST(VAL AS CHAR) AS PGCOURSE ,COURSE as PGCOURSE_mapping from PG_COURSE",login('63:3306/JOBPOSTING'))
# pg_map=pg.set_index('PGCOURSE').to_dict()['PGCOURSE_mapping']
# def pg_mapper(s):
#     if s is None:
#         return ''
#     else:
#         return ','.join([pg_map[x] if x in pg_map else str(x) for x in s.split(',')])
# df['PGCOURSE_mapped']=df['PGCOURSE'].apply(lambda s: pg_mapper(s))


df['Is_PowerHV'] = 'No'
df.loc[(df['mode'] == 'rmj') & (df['Job_Type'] == 'h'), 'Is_PowerHV'] = 'Yes'

# Finding Currently Live NFL Jobs (Usual count is <100)
jobs_mongo_conn = MongoConnectRN("naukri_nfl_jobs", database=mongo_ore_jobs_db['database'], username=mongo_ore_jobs_db['username'], password=mongo_ore_jobs_db[
                                 'password'], replica_host_port=mongo_ore_jobs_db['replica_host_port'], replicaset='RPL-JOB', readPreference='secondary')
nfl_fetched = jobs_mongo_conn.find()
nfl = pd.DataFrame(list(nfl_fetched))
nfl = nfl[nfl['endDate'] > pd.datetime.now()]
nfl = nfl.groupby('jobId').agg(
    {'keySkill': lambda x: ','.join(x.tolist())}).reset_index()
nfl.columns = ['file', 'NFL_keySkill']

df['Is_NFL'] = 'No'
df.loc[(df['file'].isin(nfl['file'])), 'Is_NFL'] = 'Yes'
df = df.merge(nfl, on='file', how='left')  # NFL Keywords


# do this as the last step
df = df.drop(['companyDetail', 'education', 'enrichedJobData', 'indexerData',
             'responseManagerSettings', 'salaryDetail', 'registrationType', 'localities'], axis=1)

# we are selecting all the 'object' columns and performing the below step to ensure no encoding error creeps in [UPDATE - 12th May, 2022]
object_dtype_cols = df.select_dtypes(include=['object']).columns.tolist()
df[object_dtype_cols] = df.select_dtypes(include=['object']).apply(
    lambda x: x.str.encode('ascii', 'ignore').str.decode('ascii'))


date = str((pd.datetime.now()-pd.Timedelta(days=1)).date())

# df['file']=df['file'].astype(int)

# u = df.select_dtypes(include=['O']) #this selects columns which are not required as well.
u = df[['referenceCode', 'title', 'COMNAME', 'companyDetail_details', 'companyDetail_websiteUrl', 'companyDetail_address',
        'CITY', 'description', 'companyUrl', 'keywords']]  # only these columns have UTF Encoding Errors sometimes.
df[u.columns] = u.apply(
    lambda x: x.str.encode('ascii', 'ignore').str.decode('ascii'))

df['keywords'] = df['keywords'].str.replace('|', ' ').str.lower()
df['description'] = df['description'].str.replace('|', ' ')
df['companyDetail_details'] = df['companyDetail_details'].str.replace('|', ' ')
df['companyDetail_websiteUrl'] = df['companyDetail_websiteUrl'].str.replace(
    '|', ' ')
df['companyDetail_address'] = df['companyDetail_address'].str.replace('|', ' ')

# df.to_parquet('/data/analytics/scheduledCodes/Naukri/Recruiter_side_reports/Jobs_Dump_Cutomer_Relation/Output/df.parquet')
# unable to export to pickle due to memory error, even parquet is not supported


# df2 = df.replace(r'\W+', '', regex=True) dont perform this step, it removes all space characters
# df['file']=df['file'].astype(str).str.zfill(12)


# OUTPUT PART

location_output = "/data/analytics/scheduledCodes/Naukri/Recruiter_side_reports/Jobs_Dump_Cutomer_Relation/Output"

filename = "{}/JobIds_Final_{}.csv".format(location_output, date)
filenamegz = "{}/JobIds_Final_{}.csv.gz".format(location_output, date)

lmn = df.to_csv(filename, sep='|', index=False)
print("@@", lmn)

f_in = open(filename, 'rb')
f_out = gzip.open(filenamegz, 'wb')
f_out.writelines(f_in)
f_out.close()
f_in.close()

##cmd="gzip -f {} > err.txt".format(filename)
# abc=os.system(cmd)
##print("@@@", abc)


cmd = "cp {} /ftpdrive/Customer_Service/MIS".format(filename+'.gz')
xyz = os.system(cmd)
print("@@@@", xyz)

########   Mailing Part   ############################################

msg = '''
Hi,\n
Please find the JobIDs for date {} at ftp://fileserverb55/Customer_Service/MIS/ .\n
File Title: {} \n
  
PS:  username=analytics2 and password=P@ssw0rd \n
  
\nRegards\n
Analytics
'''.format(date, 'JobIds_Final_{}.csv'.format(date))

subject = "Customer Relation : JobIds {}".format(date)

# mailer_list=["aman.s@naukri.com" ,"manoj.shah@naukri.com","mis@naukri.com","varsha.rani@naukri.com","noori@naukri.com","Suruchi.Bedi@naukri.com"]
mailer_list = ["bi@naukri.com", "chitralekha.sumbrui@naukri.com",
               "manish.k2@naukri.com", "mahima.kukreja@naukri.com"]

send_mail('analytics@naukri.com', mailer_list, subject, msg, [])
#### Remove CSV Files ############################################
cmd = "rm {}".format(filename)
p = os.system(cmd)
print("end@", p)

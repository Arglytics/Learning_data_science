from Datasets.pipelines.Spark.base.sessionbuilder import SparkSessionManager
from Datasets.pipelines.Spark.base.dataset import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import calendar
import pendulum
import cx_Oracle
import sys
import os
import time
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from Utils.config_wrapper.wrapper import BaseHookWrapper, VariableWrapper

con = BaseHookWrapper.get_connection("analytic")
ip = '10.1.69.143'
port = 1579
SID = 'analytic'

dsn_tns = cx_Oracle.makedsn(ip, port, SID)
conn = cx_Oracle.connect(con.login, con.password, dsn_tns)

def agency_monthly_results():
    gam_branch=f"""select sol_id, foracid,acct_name,cust_id,schm_code from ugedw.stg_gam@edw
                where schm_code in ('SB126', 'CA207','SB117')"""
    gam=pd.read_sql(gam_branch,conn)

    # agents=f"""select substr(a.ADD_INFO_03, 0,3) Branch,b.client_number "Agent_Code",b.trade_nam "Agency_Name",
    #             contract_number "Terminal_Id",b.first_nam || ' ' || b.last_nam "Agent_Name",
    #             b.phone "Agency_Contact",REPLACE ( (b.client_number), 'AG', '') "Agent_ID",        
    #             a.ADD_INFO_03  "Location",substr(a.ADD_INFO_03, 0,14)  "Outlet_Code",
    #             rbs_number "Transacting_Acc",a.ADD_INFO_02 "Commission_Acc",
    #             b.ADD_INFO_03 "KRA_PIN",a.date_open "Date Opened",
    #             b.reg_number "National ID",NVL(b.ADDRESS_LINE_1,b.ADDRESS_LINE_2) ADDRESS
    #             from  ANALYTICS_STG.STG_WAY4_CONTRACT a,
    #             ANALYTICS_STG.STG_WAY4_CLIENT b
    #             where a.amnd_state = 'A' and b.amnd_state = 'A' and a.client__id = b.id
    #             and a.client_type in( '1879', '1883', '1881') and a.TR_SIC in('6010')
    #             and a.f_i = '717' and a.contract_number not like '%-C-%'
    #             order by 13 desc"""
    # agents_details=pd.read_sql(agents,conn)

    agents=pd.read_csv('/home/user/project/Projects/Agency/Dormancy/agents.csv',engine='python')
    agents_acc=tuple(agents['Transacting_Acc'].unique().tolist())

    a=datetime.now().date()
    b=a.replace(day = 1)
    end_month = b.replace(day = calendar.monthrange(b.year, b.month)[1])
    end_month=end_month.strftime('%Y-%m-%d')


    dates = pd.date_range(end=end_month, periods=3, freq='M')
    date=dates
    def zero_pad(v):
            return "0"+str(v) if v < 10 else str(v)

    df = []
    for date in dates:
        start = time.time()

        if date==pd.to_datetime(end_month):
            tran_table = f"STATS@edw"
        else:
            tran_table = f"STATS_P{date.year}{'0'+str(date.month) if date.month < 10 else str(date.month)}"+"@edw"

        query = f"""
                (SELECT
                    date '{date.date().isoformat()}' as TRAN_month,
                    TRAN.TRAN_ID,
                    TRAN.TRAN_DATE,
                    TRAN.FORACID as AGENT_FORACID,
                    TRAN2.FORACID as CUST_FORACID,
                    TRAN.PSTD_DATE as POSTING_DATE,
                    TRAN.TRAN_AMT,
                    TRAN.TRAN_PARTICULAR,
                    TRAN.INIT_SOL_ID,
                    TRAN.DELIVERY_CHANNEL_ID,
                    TRAN.PART_TRAN_TYPE,
                    (CASE 
                        WHEN DELIVERY_CHANNEL_ID in ('WPD','EAZ')
                        THEN
                        (CASE WHEN TRAN_PARTICULAR LIKE '%CASH ADVANCE%' THEN 'WITHDRAWAL'
                        WHEN TRAN_PARTICULAR LIKE '%CASH DEPOSIT%' THEN 'DEPOSIT'
                        WHEN TRAN_PARTICULAR LIKE '%T-BY:%' THEN 'DEPOSIT'
                        WHEN TRAN_PARTICULAR LIKE '%EAZZY-WITHDRAWAL%' THEN 'WITHDRAWAL'
                        WHEN TRAN_PARTICULAR LIKE '%EAZZY-DEPOSIT%' THEN 'DEPOSIT'
                        WHEN TRAN_PARTICULAR LIKE '%EAZZY-AGENT DEPOSIT%' THEN 'DEPOSIT'
                        WHEN TRAN_PARTICULAR LIKE '%EAZZY-AGENT WITHDRAWAL%' THEN 'WITHDRAWAL'
                        WHEN TRAN_PARTICULAR LIKE '%DIBPAY%' THEN 'DEPOSIT'
                        WHEN TRAN_PARTICULAR LIKE '%C-BY%' THEN 'DEPOSIT'
                        WHEN TRAN_PARTICULAR LIKE '%BPAY%' THEN 'DEPOSIT'
                        ELSE 'OTHERS' END)
                        ELSE 'OTHERS'
                    END )TRAN_TYPE 
                FROM 
                    {tran_table} TRAN
                    LEFT JOIN (SELECT TRAN_ID,FORACID,PART_TRAN_SRL_NUM,CUST_ID,REF_AMT,TRAN_AMT FROM {tran_table}) TRAN2
                        ON TRAN.TRAN_ID=TRAN2.TRAN_ID 
                        AND TRAN.FORACID<>TRAN2.FORACID
                        AND TRAN.TRAN_AMT=-TRAN2.TRAN_AMT
                WHERE 
                    TRAN.SCHM_TYPE in ('CAA', 'SBA')
                    AND TRAN.SCHM_CODE in ('SB126', 'CA207','SB117')
                )
                
                """

        dfs = pd.DataFrame()
        data=pd.read_sql(query, conn)
        df.append(data)
        agents_trans = pd.concat(df, ignore_index=True)
        end = time.time()
        print(date, end-start)

    agents_trans1=agents_trans[['TRAN_DATE','AGENT_FORACID', 'CUST_FORACID','POSTING_DATE', 'TRAN_AMT','PART_TRAN_TYPE', 'TRAN_TYPE','TRAN_PARTICULAR']]
    agents_trans1['transaction']=np.where((agents_trans1['PART_TRAN_TYPE']=='C')&(agents_trans1['TRAN_TYPE']=='OTHERS'),\
        'credit',np.where((agents_trans1['PART_TRAN_TYPE']=='D')&(agents_trans1['TRAN_TYPE']=='OTHERS'),\
        'debit',agents_trans1['TRAN_TYPE']))

    agents_trans1['transaction']=np.where(agents_trans1['TRAN_TYPE']=='DEPOSIT',\
        'debit',np.where(agents_trans1['TRAN_TYPE']=='WITHDRAWAL',\
        'credit',agents_trans1['transaction']))

    agents_trans1['AMOUNT']=agents_trans1['TRAN_AMT'].abs()


    mydate=datetime.now().date()
    x=mydate.replace(day = 1)
    last_date_of_month = x.replace(day = calendar.monthrange(x.year, x.month)[1])

    month_end=last_date_of_month
    month_start = month_end - timedelta(days=(month_end.day-1))
    window3= month_start - pd.DateOffset(months=3)

    agents_max_min = agents_trans1[(agents_trans1.TRAN_DATE <= month_end) & (agents_trans1.TRAN_DATE >= window3)]
    agents_max_min = agents_max_min.groupby('AGENT_FORACID').agg({'TRAN_DATE':[np.min, np.max]})
    agents_max_min.columns = ['First Trx Day in last 90 days','Most Recent Trx Day in last 90 days']
    agents_max_min = agents_max_min.reset_index()
    agents_max_min['First Trx Day in last 90 days'] = agents_max_min['First Trx Day in last 90 days'].dt.date
    agents_max_min['Most Recent Trx Day in last 90 days'] = agents_max_min['Most Recent Trx Day in last 90 days'].dt.date

    agents_trans2=agents_trans1

    agents_thismonth = agents_trans2[(agents_trans2.TRAN_DATE <= month_end) & (agents_trans2.TRAN_DATE >= month_start)]

    agents_thismonth_agg = agents_thismonth.groupby(['AGENT_FORACID']).size().reset_index(name='TOTAL_TRX')


    data5=pd.pivot_table(data=agents_thismonth, index=['AGENT_FORACID'], columns='transaction', values='AMOUNT', aggfunc=['sum','min','max']).reset_index()
    data5.columns = ["_".join(x) for x in data5.columns.ravel()]
    data5.columns = ['AGENT_FORACID', 'CURR_TOTAL_INFLOW', 'CURR_TOTAL_OUTFLOW', 'CURR_MIN_AMT_IN','CURR_MIN_AMT_OUT','CURR_MAX_AMT_IN','CURR_MAX_AMT_OUT']


    agents_thismonth_agg1=pd.merge(agents_thismonth_agg,data5,on='AGENT_FORACID',how='inner')
    agents_thismonth_agg1=agents_thismonth_agg1.rename(columns={'AGENT_FORACID':'AGENT_FOR_THISmonth'})


    agents_beforethismonth = agents_trans2[(agents_trans2.TRAN_DATE < month_start) & (agents_trans2.TRAN_DATE >= window3)]
    agents_beforethismonth["POSTING_month"] = agents_beforethismonth["TRAN_DATE"].dt.month

    agents_beforethismonth_agg1 = agents_beforethismonth.groupby(['AGENT_FORACID']).agg({'POSTING_month':lambda x: x.nunique()}).reset_index()
    agents_beforethismonth_agg1.columns = ['AGENT_FORACID','TOTAL_monthS_ACTIVE']

    data6=pd.pivot_table(data=agents_beforethismonth, index=['AGENT_FORACID'], columns='transaction', values='AMOUNT', aggfunc=['sum','min','max'])
    data6.columns = ["_".join(x) for x in data6.columns.ravel()]
    data6=data6.reset_index()
    data6.columns = ['AGENT_FORACID', 'PREV_TOTAL_INFLOW', 'PREV_TOTAL_OUTFLOW', 'PREV_MIN_AMT_IN','PREV_MIN_AMT_OUT','PREV_MAX_AMT_IN','PREV_MAX_AMT_OUT']

    agents_beforethismonth_agg2=pd.merge(agents_beforethismonth_agg1,data6,on='AGENT_FORACID',how='inner')
    agents_beforethismonth_agg2['AVG_monthLY_INFLOW']=agents_beforethismonth_agg2['PREV_TOTAL_INFLOW']/agents_beforethismonth_agg2['TOTAL_monthS_ACTIVE']
    agents_beforethismonth_agg2['AVG_monthLY_OUTFLOW']=agents_beforethismonth_agg2['PREV_TOTAL_OUTFLOW']/agents_beforethismonth_agg2['TOTAL_monthS_ACTIVE']

    summary1=pd.merge(agents_beforethismonth_agg2,agents_thismonth_agg1,left_on='AGENT_FORACID',right_on='AGENT_FOR_THISmonth',how='outer')

    agency_monthly_results = pd.merge(summary1,agents_max_min,on='AGENT_FORACID',how='right')
    agency_monthly_results2 = agency_monthly_results[~agency_monthly_results['TOTAL_monthS_ACTIVE'].isna()]
    agency_monthly_results3 = agency_monthly_results2[agency_monthly_results2['AGENT_FOR_THISmonth'].isna()]

    agency_monthly_results3=agency_monthly_results3[['AGENT_FORACID', 'TOTAL_monthS_ACTIVE', 'PREV_TOTAL_INFLOW',
        'PREV_TOTAL_OUTFLOW', 'PREV_MIN_AMT_IN', 'PREV_MIN_AMT_OUT',
        'PREV_MAX_AMT_IN', 'PREV_MAX_AMT_OUT', 'AVG_monthLY_INFLOW',
        'AVG_monthLY_OUTFLOW','First Trx Day in last 180 days',
        'Most Recent Trx Day in last 180 days']]

    months_active_col = f'Number of months active before {month_start.isoformat()}'
    totalinmonthincol = f'Total inflow amount between {window3.isoformat()} and {month_start.isoformat()}'
    totaloutmonthoutcol = f'Total outflow amount between {window3.isoformat()} and {month_start.isoformat()}'
    min_inmonthincol = f'Minimum inflow amount between {window3.isoformat()} and {month_start.isoformat()}'
    min_outmonthoutcol = f'Minimum outflow amount between {window3.isoformat()} and {month_start.isoformat()}'
    max_inmonthincol = f'Maximum inflow amount between {window3.isoformat()} and {month_start.isoformat()}'
    max_outmonthoutcol = f'Maximum outflow amount between {window3.isoformat()} and {month_start.isoformat()}'
    avgmonthincol = f'monthly Average inflow amount between {window3.isoformat()} and {month_start.isoformat()}'
    avgmonthoutcol = f'monthly Average outflow amount between {window3.isoformat()} and {month_start.isoformat()}'

    mx_tran_date=f'Most Recent Trx Day in last 90 days'
    mn_tran_date=f'First Trx Day in last 90 days'

    columns = ['AGENT_FORACID', months_active_col,totalinmonthincol,totaloutmonthoutcol,min_inmonthincol,
    min_outmonthoutcol,max_inmonthincol,max_outmonthoutcol,avgmonthincol,avgmonthoutcol,
    mn_tran_date,mx_tran_date]

    agency_monthly_results3.columns = columns


    agents_details1=agents.drop_duplicates(subset='Transacting_Acc')

    agency_month_results5=pd.merge(agency_monthly_results3,agents_details1[['BRANCH','Agent_Code','Agency_Name','Agent_Name','Transacting_Acc','Terminal_Id']]
                            ,left_on='AGENT_FORACID',right_on='Transacting_Acc',how='left')

    agency_month_results5=agency_month_results5.drop(['Transacting_Acc'],axis=1)

    agency_month_results5=agency_month_results5.rename(columns={'AGENT_FORACID':'Agent_Acc_Number'})
    agency_month_results5=agency_month_results5[['Agent_Acc_Number','BRANCH', 'Agent_Code', 'Agency_Name', 'Agent_Name',
                                                'Terminal_Id',months_active_col,totalinmonthincol,totaloutmonthoutcol,min_inmonthincol,
                                                    min_outmonthoutcol,max_inmonthincol,max_outmonthoutcol,avgmonthincol,avgmonthoutcol,
                                                    mn_tran_date,mx_tran_date]]

    not_exists=agency_month_results5.loc[agency_month_results5['Agent_Code'].isnull()]
    not_exists.to_csv('null_agents_code.csv')

    agency_month_results6=agency_month_results5.loc[~agency_month_results5['Agent_Code'].isnull()]
    y=pd.merge(agency_month_results6,gam,left_on='Agent_Acc_Number',right_on='FORACID',how='left')
    agency_month_results6['BRANCH']=np.where(agency_month_results6['BRANCH'].isnull(),y['SOL_ID'],agency_month_results6['BRANCH'])

    branch_emails=pd.read_excel('/home/user/project/Projects/Agency/Dormancy/2020_MAY_BRANCH-AGENCY_TEAM.xlsx')
    branch_emails=branch_emails.rename(columns={'SOL':'BRANCH_ID'})
    branch_emails=branch_emails[['BRANCH_ID', 'Branch', 'Region', 'PF', 'Email address',
        'BGDM', 'OPS']]
    branch_emails.columns=['BRANCH_ID','BRANCH','REGION','PF','STAFF_EMAIL','BGDM_EMAIL','OPS_EMAIL']
    branch_emails['BRANCH_ID']=np.where(branch_emails['BRANCH']=='Emali',181.0,branch_emails['BRANCH_ID'])


    regional_managers = pd.read_excel("/home/user/project/Projects/Agency/Dormancy/regional support.xlsx",sheet_name='Branch Staff')
    regional_managers=regional_managers.rename(columns={'SOL':'BRANCH_ID'})
    regional_managers.columns=['BRANCH_ID','BRANCH','REGION','RM','RM_EMAIL']


    managers_info=pd.merge(branch_emails,regional_managers[['BRANCH_ID', 'RM', 'RM_EMAIL']],on='BRANCH_ID',how='left')
    managers_info=managers_info.drop_duplicates(subset='BRANCH_ID')

    agency_month_results6['BRANCH']=agency_month_results6['BRANCH'].str.strip("'")
    agency_month_results6['BRANCH']=agency_month_results6['BRANCH'].str.strip("-")
    agency_month_results6['BRANCH']=agency_month_results6['BRANCH'].str.lstrip('0')

    agency_month_results6['BRANCH']=agency_month_results6['BRANCH'].astype(float)
    agency_month_results6=agency_month_results6.rename(columns={'BRANCH':'BRANCH_ID'})

    agents_monthly_results6 = pd.merge(agency_month_results6, managers_info, on='BRANCH_ID', how='left')
    agents_monthly_results6.loc[agents_monthly_results6['RM_EMAIL'].isnull()]

    return agents_monthly_results6


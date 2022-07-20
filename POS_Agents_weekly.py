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


def pos_agency_weekly_results():
    con = BaseHookWrapper.get_connection("analytic")
    ip = '10.1.69.143'
    port = 1579
    SID = 'analytic'

    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    conn = cx_Oracle.connect(con.login, con.password, dsn_tns)

    # agent details
    agents = f"""select substr(a.ADD_INFO_03, 0,3) as Branch, 
                b.client_number as Agent_Code,
                b.trade_nam as Agency_Name,
                contract_number as Terminal_Id, 
                b.first_nam || ' ' || b.last_nam as Agent_Name,
                b.phone as Agency_Contact,
                REPLACE ( (b.client_number), 'AG', '') as Agent_ID,
                substr(a.ADD_INFO_03, 0,14) as Outlet_Code, 
                rbs_number as Transacting_Acc,
                a.ADD_INFO_02 as Commission_Acc
                from ANALYTICS_STG.STG_WAY4_CONTRACT a,
                ANALYTICS_STG.STG_WAY4_CLIENT b 
                where a.amnd_state = 'A'
                and b.amnd_state = 'A'
                and a.client__id = b.id
                and a.client_type in( '1879', '1883', '1881')
                and a.MERCHANT_ID in('6010')
                and a.f_i = '717'
                and a.contract_number not like '%-C-%'
                """
    agent_details = pd.read_sql(agents, conn)
    ###############################
    a = datetime.now().date()
    b = a.replace(day=1)
    end_month = b.replace(day=calendar.monthrange(b.year, b.month)[1])
    end_month = end_month.strftime('%Y-%m-%d')
    dates = pd.date_range(end=end_month, periods=4, freq='M')
    date = dates

    def zero_pad(v):
        return "0"+str(v) if v < 10 else str(v)

    df = []
    for date in dates:
        start = time.time()

        if date == pd.to_datetime(end_month):
            tran_table = f"STATS@edw"
        else:
            tran_table = f"STATS_P{date.year}{'0'+str(date.month) if date.month < 10 else str(date.month)}"+"@edw"

        query = f"""
                select PSTD_DATE,TRAN_DATE,tran_id, foracid,SCHM_CODE, CUST_ID, REF_AMT,ACCT_NAME,
                    TRAN_PARTICULAR, TRAN_PARTICULAR_2 TID,PART_TRAN_TYPE
                    from {tran_table}
                    WHERE SCHM_TYPE in ('SBA','CAA') and
                    length(TRAN_PARTICULAR_2) = 8 
                    and (TRAN_PARTICULAR LIKE '%CASH DEPOSIT/%' or TRAN_PARTICULAR  LIKE '%T-BY:%' 
                    or TRAN_PARTICULAR  LIKE '%C-BY:%' or TRAN_PARTICULAR  LIKE '%BPAY:%' or TRAN_PARTICULAR LIKE '%CASH ADVANCE/%')
                                            
            """
        dfs = pd.DataFrame()
        data = pd.read_sql(query, conn)
        df.append(data)
        agents_pos = pd.concat(df, ignore_index=True)
        end = time.time()
        print(date, end-start)

    agents_trans1 = agents_pos
    agents_trans1.columns
    agents_trans1 = agents_pos[['TRAN_DATE', 'FORACID', 'CUST_ID',
                                'REF_AMT', 'ACCT_NAME', 'TID', 'PART_TRAN_TYPE']]
    agents_trans1['transaction_type'] = np.where(agents_trans1['PART_TRAN_TYPE'] == 'D',
                                                 'deposits', np.where(agents_trans1['PART_TRAN_TYPE'] == 'C',
                                                                      'withdrawals', agents_trans1['PART_TRAN_TYPE']))

    mydate = datetime.now().date()
    week_start = mydate - timedelta(days=7)
    week_end = mydate - timedelta(days=1)
    window3 = pd.to_datetime(week_start - pd.DateOffset(months=3)).date()

    agents_max_min = agents_trans1[(agents_trans1.TRAN_DATE <= week_end) & (
        agents_trans1.TRAN_DATE >= window3)]
    agents_max_min = agents_max_min.groupby(
        'TID').agg({'TRAN_DATE': [np.min, np.max]})
    agents_max_min.columns = [
        'First Trx Day in last 90 days', 'Most Recent Trx Day in last 90 days']
    agents_max_min = agents_max_min.reset_index()
    agents_max_min['First Trx Day in last 90 days'] = agents_max_min['First Trx Day in last 90 days'].dt.date
    agents_max_min['Most Recent Trx Day in last 90 days'] = agents_max_min['Most Recent Trx Day in last 90 days'].dt.date

    agents_trans2 = agents_trans1

    agents_trans2 = agents_trans2.rename(columns={'REF_AMT': 'AMOUNT'})

    agents_thisweek = agents_trans2[(agents_trans2.TRAN_DATE <= week_end) & (
        agents_trans2.TRAN_DATE >= week_start)]

    agents_thisweek_agg = agents_thisweek.groupby(
        ['TID']).size().reset_index(name='TOTAL_TRX')

    data5 = pd.pivot_table(data=agents_thisweek, index=[
                           'TID'], columns='transaction_type', values='AMOUNT', aggfunc=['sum', 'min', 'max']).reset_index()
    data5.columns = ["_".join(x) for x in data5.columns.ravel()]
    data5.columns = ['TID', 'CURR_TOTAL_DEPOSITS', 'CURR_TOTAL_WITH',
                     'CURR_MIN_AMT_DEP', 'CURR_MIN_AMT_WITH', 'CURR_MAX_AMT_DEP', 'CURR_MAX_AMT_WITH']

    agents_thisweek_agg1 = pd.merge(
        agents_thisweek_agg, data5, on='TID', how='inner')
    agents_thisweek_agg1 = agents_thisweek_agg1.rename(
        columns={'TID': 'TID_FOR_THISWEEK'})

    agents_beforethisweek = agents_trans2[(
        agents_trans2.TRAN_DATE < week_start) & (agents_trans2.TRAN_DATE >= window3)]
    agents_beforethisweek["POSTING_WEEK"] = agents_beforethisweek["TRAN_DATE"].dt.week

    agents_beforethisweek_agg1 = agents_beforethisweek.groupby(
        ['TID']).agg({'POSTING_WEEK': lambda x: x.nunique()}).reset_index()
    agents_beforethisweek_agg1.columns = ['TID', 'TOTAL_WEEKS_ACTIVE']

    data6 = pd.pivot_table(data=agents_beforethisweek, index=[
                           'TID'], columns='transaction_type', values='AMOUNT', aggfunc=['sum', 'min', 'max'])
    data6.columns = ["_".join(x) for x in data6.columns.ravel()]
    data6 = data6.reset_index()
    data6.columns = ['TID', 'PREV_TOTAL_DEPOSITS', 'PREV_TOTAL_WITH',
                     'PREV_MIN_AMT_DEP', 'PREV_MIN_AMT_WITH', 'PREV_MAX_AMT_DEP', 'PREV_MAX_AMT_WITH']

    agents_beforethisweek_agg2 = pd.merge(
        agents_beforethisweek_agg1, data6, on='TID', how='inner')
    agents_beforethisweek_agg2['AVG_WEEKLY_DEPOSITS'] = agents_beforethisweek_agg2['PREV_TOTAL_DEPOSITS'] / \
        agents_beforethisweek_agg2['TOTAL_WEEKS_ACTIVE']
    agents_beforethisweek_agg2['AVG_WEEKLY_WITHDRAWALS'] = agents_beforethisweek_agg2['PREV_TOTAL_WITH'] / \
        agents_beforethisweek_agg2['TOTAL_WEEKS_ACTIVE']

    summary1 = pd.merge(agents_beforethisweek_agg2, agents_thisweek_agg1,
                        left_on='TID', right_on='TID_FOR_THISWEEK', how='outer')

    agency_weekly_results = pd.merge(
        summary1, agents_max_min, on='TID', how='right')
    agency_weekly_results2 = agency_weekly_results[~agency_weekly_results['TOTAL_WEEKS_ACTIVE'].isna(
    )]
    agency_weekly_results3 = agency_weekly_results2[agency_weekly_results2['TID_FOR_THISWEEK'].isna(
    )]

    agency_weekly_results3 = agency_weekly_results3[['TID', 'TOTAL_WEEKS_ACTIVE', 'PREV_TOTAL_DEPOSITS',
                                                     'PREV_TOTAL_WITH', 'PREV_MIN_AMT_DEP', 'PREV_MIN_AMT_WITH',
                                                     'PREV_MAX_AMT_DEP', 'PREV_MAX_AMT_WITH', 'AVG_WEEKLY_DEPOSITS',
                                                     'AVG_WEEKLY_WITHDRAWALS', 'First Trx Day in last 90 days',
                                                     'Most Recent Trx Day in last 90 days']]

    weeks_active_col = f'Number of weeks active before {week_start.isoformat()}'
    totalinweekincol = f'Total deposits amount between {window3.isoformat()} and {week_start.isoformat()}'
    totaloutweekoutcol = f'Total withdrawn amount between {window3.isoformat()} and {week_start.isoformat()}'
    min_inweekincol = f'Minimum deposits amount between {window3.isoformat()} and {week_start.isoformat()}'
    min_outweekoutcol = f'Minimum withdrawn amount between {window3.isoformat()} and {week_start.isoformat()}'
    max_inweekincol = f'Maximum deposits amount between {window3.isoformat()} and {week_start.isoformat()}'
    max_outweekoutcol = f'Maximum withdrawn amount between {window3.isoformat()} and {week_start.isoformat()}'
    avgweekincol = f'Weekly Average deposited amount between {window3.isoformat()} and {week_start.isoformat()}'
    avgweekoutcol = f'Weekly Average withdrawn amount between {window3.isoformat()} and {week_start.isoformat()}'

    mx_tran_date = f'Most Recent Trx Day in last 90 days'
    mn_tran_date = f'First Trx Day in last 90 days'

    columns = ['TID', weeks_active_col, totalinweekincol, totaloutweekoutcol, min_inweekincol,
               min_outweekoutcol, max_inweekincol, max_outweekoutcol, avgweekincol, avgweekoutcol,
               mn_tran_date, mx_tran_date]

    agency_weekly_results3.columns = columns

    agents_details1 = agent_details

    agency_week_results5 = pd.merge(agency_weekly_results3, agents_details1[[
                                    'BRANCH', 'AGENT_CODE', 'AGENCY_NAME', 'AGENT_NAME', 'TRANSACTING_ACC', 'TERMINAL_ID', 'OUTLET_CODE']], left_on='TID', right_on='TERMINAL_ID', how='left')

    agency_week_results5 = agency_week_results5[['TRANSACTING_ACC', 'BRANCH', 'AGENT_CODE', 'AGENCY_NAME', 'AGENT_NAME', 'OUTLET_CODE',
                                                 'TERMINAL_ID', weeks_active_col, totalinweekincol, totaloutweekoutcol, min_inweekincol,
                                                 min_outweekoutcol, max_inweekincol, max_outweekoutcol, avgweekincol, avgweekoutcol,
                                                 mn_tran_date, mx_tran_date]]

    not_exists = agency_week_results5.loc[agency_week_results5['AGENT_CODE'].isnull(
    )]
    not_exists.to_csv(
        '/home/user/project/Projects/Agency/Dormancy/null_agents_code.csv')

    not_exists_TID = agency_week_results5.loc[agency_week_results5['TRANSACTING_ACC'].isnull(
    )]
    not_exists_TID.to_csv(
        '/home/user/project/Projects/Agency/Dormancy/null_TIDs.csv')

    agency_week_results5 = agency_week_results5.loc[~agency_week_results5['TRANSACTING_ACC'].isnull(
    )]

    agency_week_results6 = agency_week_results5

    # gam
    branches = agency_week_results6.loc[agency_week_results6['BRANCH'].isnull(
    )]

    if branches.empty:
        print('dataframe is empty')
    else:
        trans_acc = tuple(branches['TRANSACTING_ACC'].unique().tolist())
        gam_branch = f"""select sol_id, foracid,acct_name,cust_id,schm_code from ugedw.stg_gam@edw
                        where foracid in {trans_acc}"""
        gam = pd.read_sql(gam_branch, conn)
        y = pd.merge(agency_week_results6, gam,
                     left_on='TRANSACTING_ACC', right_on='FORACID', how='left')
        agency_week_results6['BRANCH'] = np.where(
            agency_week_results6['BRANCH'].isnull(), y['SOL_ID'], agency_week_results6['BRANCH'])

    branch_emails = pd.read_excel(
        '/home/user/project/Projects/Agency/Dormancy/2020_MAY_BRANCH-AGENCY_TEAM.xlsx')
    branch_emails = branch_emails.rename(columns={'SOL': 'BRANCH_ID'})
    branch_emails = branch_emails[['BRANCH_ID', 'Branch', 'Region', 'PF', 'Email address',
                                   'BGDM', 'OPS']]
    branch_emails.columns = ['BRANCH_ID', 'BRANCH', 'REGION',
                             'PF', 'STAFF_EMAIL', 'BGDM_EMAIL', 'OPS_EMAIL']
    branch_emails['BRANCH_ID'] = np.where(
        branch_emails['BRANCH'] == 'Emali', 181.0, branch_emails['BRANCH_ID'])
    branch_emails = branch_emails.dropna(subset=['BRANCH_ID'])
    branch_emails['BRANCH_ID'] = branch_emails['BRANCH_ID'].astype(int)

    def zero_pad(x):
        if x < 10:
            return '00'+str(x)
        elif 10 <= x < 100:
            return '0'+str(x)
        else:
            return str(x)
    branch_emails['BRANCH_ID'] = branch_emails['BRANCH_ID'].apply(
        lambda x: zero_pad(x))

    regional_managers = pd.read_csv(
        "/home/user/project/Projects/Merchant/dormancyreports/rm_list.csv")

    regional_managers['SOL'] = [x[:3] for x in regional_managers['BRANCH']]
    regional_managers = regional_managers[['SOL', 'BRANCH', 'RM', 'RM_EMAIL']]
    regional_managers['SOL'] = regional_managers['SOL'].astype(int)

    def zero_pad(x):
        if x < 10:
            return '00'+str(x)
        elif 10 <= x < 100:
            return '0'+str(x)
        else:
            return str(x)
    regional_managers['SOL'] = regional_managers['SOL'].apply(
        lambda x: zero_pad(x))

    regional_managers.columns = ['BRANCH_ID', 'BRANCH', 'RM', 'RM_EMAIL']

    managers_info = pd.merge(branch_emails, regional_managers[[
                             'BRANCH_ID', 'RM', 'RM_EMAIL']], on='BRANCH_ID', how='outer')
    managers_info = managers_info.drop_duplicates(subset='BRANCH_ID')
    managers_info = managers_info.loc[~managers_info['BRANCH_ID'].isnull()]

    agency_week_results6['BRANCH'] = agency_week_results6['BRANCH'].str.strip(
        "'")
    agency_week_results6['BRANCH'] = agency_week_results6['BRANCH'].str.strip(
        "`")
    agency_week_results6['BRANCH'] = agency_week_results6['BRANCH'].str.strip(
        "-")
    agency_week_results6['BRANCH'] = agency_week_results6['BRANCH'].str.replace('[^\w\s]','')
    agency_week_results6['BRANCH'] = agency_week_results6['BRANCH'].astype(int)

    def zero_pad(x):
        if x < 10:
            return '00'+str(x)
        elif 10 <= x < 100:
            return '0'+str(x)
        else:
            return str(x)

    agency_week_results6['BRANCH'] = agency_week_results6['BRANCH'].apply(
        lambda x: zero_pad(x))

    agency_week_results6 = agency_week_results6.rename(
        columns={'BRANCH': 'BRANCH_ID'})

    agents_weekly_results6 = pd.merge(
        agency_week_results6, managers_info, on='BRANCH_ID', how='left')

    # gam
    branchID = agents_weekly_results6.loc[agents_weekly_results6['BRANCH'].isnull(
    )]

    if branchID.empty:
        print('dataframe is empty')
    else:
        ids_branch = f"""select SOL_ID,SOL_DESC from ugedw.stg_sol@edw
                """
        ids_branch = pd.read_sql(ids_branch, conn)
        ids_branch['SOL_ID'] = ids_branch['SOL_ID'].dropna()

        y = pd.merge(agents_weekly_results6, ids_branch,
                     left_on='BRANCH_ID', right_on='SOL_ID', how='left')
        agents_weekly_results6['BRANCH'] = np.where(agents_weekly_results6['BRANCH'].isnull(
        ), y['SOL_DESC'], agents_weekly_results6['BRANCH'])

    agents_weekly_results6['BRANCH'] = agents_weekly_results6['BRANCH'].str.lower(
    )

    return agents_weekly_results6

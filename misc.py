import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import cx_Oracle
import numpy as np
import glob
import os
from collections.abc import Iterable
import Projects.credit_scoring_202102.base.Properties as props
import time

c = props.ProcessDataDbToFile()
# Get DB Connections
conn = c.get_db_conn()
conn_ebods = c.get_db_conn_ebods()

df = pd.read_excel("C:/Users/juliet.ondisi/Desktop/creditscoring/uganda/LOAN_SCHEME_CODES.xlsx")
df.head()
df['SCHM_CODE'] = df['SCHM_CODE'].astype(str)

query = f"""select schm_code,count(*) from ugedw.stg_gam@edw
            where bank_id='56' and acct_opn_date>='01-JAN-2021' and acct_opn_date<='31-DEC-2021'
            and SCHM_TYPE='LAA'
            group by schm_code"""
loans = pd.read_sql(query,conn)
loans['SCHM_CODE'] = loans['SCHM_CODE'].astype(str)
loans.head()

dt = loans[loans['SCHM_CODE'].isin(df.SCHM_CODE)]
dt.head()
dt.rename(columns={'COUNT(*)':'NUMBER_OF_LOANS'},inplace=True)
dt['NUMBER_OF_LOANS'] = dt['NUMBER_OF_LOANS'].astype(int)

dt1 = pd.merge(df,dt,on='SCHM_CODE',how='left')
dt1.head()
dt1['NUMBER_OF_LOANS'] = dt1['NUMBER_OF_LOANS'].fillna(0)
dt1['NUMBER_OF_LOANS'] = dt1['NUMBER_OF_LOANS'].astype(int)

dt1.to_csv("C:/Users/juliet.ondisi/Desktop/creditscoring/uganda/ug_loan_count.csv",index=False)


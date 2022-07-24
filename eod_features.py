import pandas as pd
import numpy as np
import time
import Projects.credit_scoring_202102.base.Properties as props
import Projects.credit_scoring_202102.base.custom_data_models as datas
import Projects.credit_scoring_202102.code.uganda.dataset_definitions as datas2
import gc
from datetime import datetime
from dateutil.relativedelta import relativedelta
import cx_Oracle
import numpy as np
enddate = pd.to_datetime("2020-05-31")

class EodFeatures():
    def __init__(self):
        self.data_dir = props.uganda_data_path

    def eod_features(self, enddate):
        
        try:
            start = time.time()
            d = datas2.Metadata()
            start_date = enddate.replace(day=1)
            #eod_balance = d.eod_data(enddate)
             
            #If using csvs : use only as a last option
            dates2 = pd.date_range(end=start_date,periods = 12,freq='M')
            eod_list = []
            for dtx in dates2:
                df = pd.read_csv(self.data_dir+"eod_data/"+dtx.date().isoformat()+".csv",
                        low_memory=False, converters={'CIF_ID': lambda x: str(x)})
                eod_list.append(df)
            
            eod_balance = pd.concat(eod_list, 0)

            eod_balance = eod_balance[~eod_balance['CIF_ID'].isnull()]
            eod_balance = eod_balance.replace(r'^\s*$',np.NaN,regex=True)
            eod_balance = eod_balance.dropna()
            
            eod_balance1 = eod_balance.copy()
            eod_balance1['TRAN_MONTH'] = eod_balance1['TRAN_MONTH'].str[7:]

            start_date = enddate.replace(day=1)
            eod_dates = pd.date_range(end=start_date, periods=12, freq='M')
            eod_month = []
            for date1 in eod_dates:
                enddate_p = date1.strftime('%Y%m')
                eod_month.append(enddate_p)
                
            cifs = eod_balance1['CIF_ID'].unique()
            idx = pd.MultiIndex.from_product((eod_month, cifs))
            
            b = eod_balance1.set_index(
                ['TRAN_MONTH', 'CIF_ID']).reindex(idx).reset_index()
            b = b.rename(columns={
                'level_0': 'TRAN_MONTH', 'level_1': 'CIF_ID'})
                
            cols = ['CIF_ID', 'TRAN_MONTH']
            for col in cols:
                b[col] = b[col].astype(str)
                eod_balance1[col] = eod_balance1[col].astype(str)
                
            eod_total1 = pd.merge(b[['CIF_ID', 'TRAN_MONTH']], eod_balance1, on=[
                'CIF_ID', 'TRAN_MONTH'], how='left')
            eod_total1 = eod_total1.fillna(0)
            
            startdate_p = enddate.strftime('%Y%m')
            
            startdate_12_p = (enddate - pd.DateOffset(months=12)).strftime('%Y%m')
            startdate_6_p = (enddate - pd.DateOffset(months=6)).strftime('%Y%m')
            startdate_3_p = (enddate - pd.DateOffset(months=3)).strftime('%Y%m')
            
            eod_12 = eod_total1[
                (eod_total1['TRAN_MONTH'] >= startdate_12_p) & (eod_total1['TRAN_MONTH'] < startdate_p)]
                
            eod_agg12 = eod_12.groupby('CIF_ID').agg(
                {'AVG_BALANCE': ['mean', 'median', 'std']})
                
            eod_agg12.columns = [x1 + "_" + x2 +
                                "_12M" for x1, x2 in eod_agg12.columns]
            eod_agg12 = eod_agg12.reset_index()
            
            eod_6 = eod_total1[
                (eod_total1['TRAN_MONTH'] >= startdate_6_p) & (eod_total1['TRAN_MONTH'] < startdate_p)]
            eod_agg6 = eod_6.groupby('CIF_ID').agg(
                {'AVG_BALANCE': ['mean', 'median', 'std']})
                
            eod_agg6.columns = [x1 + "_" + x2 +
                                "_6M" for x1, x2 in eod_agg6.columns]
            eod_agg6 = eod_agg6.reset_index()
            
            eod_3 = eod_total1[
                (eod_total1['TRAN_MONTH'] >= startdate_3_p) & (eod_total1['TRAN_MONTH'] < startdate_p)]
                
            eod_agg3 = eod_3.groupby('CIF_ID').agg(
                {'AVG_BALANCE': ['mean', 'median', 'std']})
                
            eod_agg3.columns = [x1 + "_" + x2 +
                                "_3M" for x1, x2 in eod_agg3.columns]
            eod_agg3 = eod_agg3.reset_index()
            
            combined = pd.merge(eod_agg12, eod_agg6, how='outer', on='CIF_ID')
            combined1 = pd.merge(combined, eod_agg3, how='outer', on='CIF_ID')
            combined1 = combined1.fillna(0)
            combined1.columns = combined1.columns.str.upper()
            
            print(combined1.head(5))
            end = time.time()
            print("eod_features data - shape:", combined1.shape, 'duration:', end - start, "seconds")
            return combined1
        except Exception as e:
            print(e)

if __name__ == '__main__':
    start = time.time()
    c = EodFeatures()
    end = time.time()
    print('Total Time taken', end - start)
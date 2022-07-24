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
from multiprocessing import Pool

"""This script generated transaction features that will be used in the feature merge script as part of the features
for the model.The features are for data that span 12,6,3 and 1 month. The main features generated are mean, median,
average, standard deviation and 75th percentile"""

"@Creator = Juliet Ondisi"


class TranFeatures:
    def __init__(self):
        self.data_dir = props.uganda_data_path
    
    def funds_trans(self, enddate):
        """
        Generate transaction & funds transfer features from data
        """
        print("Start executing funds_trans()")
        try:
            # print(enddate)
            start = time.time()
            # print("step 1: start")
            d = datas2.Metadata()

            print("step 2: fetch currency codes")
            currency = d.currency_codes()
            date_cols = ['RTLIST_DATE']
            # currency = pd.read_csv(
            #     self.data_dir+"others/currency_codes.csv", parse_dates=date_cols)

            # print("step 3: fetch gam data")
            gam = d.gamdata()
            col_types = {'CIF_ID': 'str'}
            # gam = pd.read_csv(self.data_dir+"others/gam_data.csv",
            #                   converters={'CIF_ID': lambda x: str(x),
            #                               'FORACID': lambda x: str(x)})

            print("step 4: fetch funds transfer data")
            ft = d.fund_transfer(enddate)

            print("step 5: process funds transfer features")
            
            #if using csv:use only as a last option
            # start_date = enddate.replace(day=1)
            # dates2 = pd.date_range(end=start_date,periods = 1,freq='M')
            # ft_list = []
            # for dt in dates2:
            #     df = pd.read_csv(self.data_dir +"funds_transfer/raw_data/"+dt.date().isoformat()+".csv",
            #                 low_memory=False,converters={'FTCIF': lambda x: str(x)})
            #     df['ENDING_PERIOD'] = pd.to_datetime(dt + pd.offsets.MonthEnd(0))
            #     ft_list.append(df)
            # ft = pd.concat(ft_list, 0)
            # ft = ft[ft.FTCIF == '50620059976']

            # ft = pd.read_csv(self.data_dir+"funds_transfer/"+enddate.date().isoformat()+".csv")
            currency.rename(columns={'FXD_CRNCY_CODE':'FTCURRCODE','RTLIST_DATE':'ENDING_PERIOD'},inplace=True)
            mnth = len(ft['ENDING_PERIOD'].unique())

            # print("step 6: merge ft & currency")
            ft1 = pd.merge(ft, currency, on=['ENDING_PERIOD','FTCURRCODE'], how='left')
            ft1['FTTO'] = ft1['FTTO'].astype(str)

            # print("step 7: merge ft1 & gam")
            ft1 = pd.merge(ft1, gam, left_on='FTTO', right_on='FORACID', how='left')
            ft1['FTAMOUNT'] = ft1['FTAMOUNT'].astype(float)
            ft1['FTAMOUNT_RWF'] = ft1['FTAMOUNT']*ft1['VAR_CRNCY_UNITS']
            
            unique_sent = ft1.groupby('FTCIF').agg(UNIQUE_TRX_SENT=('FTTO','nunique'),UNIQUE_VALUE_COUNT_SENT=('FTAMOUNT','nunique')).reset_index()
            unique_sent.rename(columns={'FTCIF':'CIF_ID'},inplace=True)
            uniq_rcved =ft1.groupby('CIF_ID').agg(
                UNIQUE_TRX_RCVD=('FTFROM','nunique'),UNIQUE_VALUE_COUNT_RCVD=('FTAMOUNT','nunique')).reset_index()
            # print("step 8: merge unique_sent & uniq_rcved")
            ft2 = pd.merge(unique_sent,uniq_rcved,
                           how='outer',on='CIF_ID')
            ft2 =ft2.fillna(0)
            
            rcv = ft1[~ft1['CIF_ID'].isnull()].reset_index(drop=True)
            r = rcv[['FTFROM','CIF_ID','FTAMOUNT']].rename(columns = {'FTFROM':'COUNTERPARTY'})
            r['TYPE'] = 'C'
            sent = ft1[~ft1['FTCIF'].isnull()].reset_index(drop=True)
            s = sent[['FTTO','FTCIF','FTAMOUNT']].rename(columns = {'FTTO':'COUNTERPARTY','FTCIF':'CIF_ID'})
            s['TYPE'] = 'D'
            t = pd.concat([r,s], ignore_index=True, join='inner')
            comb = t.groupby('CIF_ID').agg(TOTAL_UNIQUE_IDS=('COUNTERPARTY','nunique'),TOT_UNIQUE_AMTS=('FTAMOUNT','nunique'))
            comb1 = pd.merge(comb,ft2,on='CIF_ID',how='inner')
            comb1 = comb1.fillna(0)

            ##mitgate infinity values
            comb1['UNIQUE_TRX_RCVD1'] = comb1['UNIQUE_TRX_RCVD']+0.000001
            comb1['UNIQUE_TRX_SENT1'] = comb1['UNIQUE_TRX_SENT']+0.000001
                
            comb1['RATIO_UR_US'] = comb1['UNIQUE_TRX_RCVD1']/comb1['UNIQUE_TRX_SENT1']
            
            comb1['UNIQUE_VALUE_COUNT_RCVD1'] = comb1['UNIQUE_VALUE_COUNT_RCVD']+0.000001
            comb1['UNIQUE_VALUE_COUNT_SENT1'] = comb1['UNIQUE_VALUE_COUNT_SENT']+0.000001
            
            comb1['RATIO_UVR_UVS'] = comb1['UNIQUE_VALUE_COUNT_RCVD1']/comb1['UNIQUE_VALUE_COUNT_SENT1']
            comb1.drop(['UNIQUE_TRX_RCVD1','UNIQUE_TRX_SENT1','UNIQUE_VALUE_COUNT_RCVD1','UNIQUE_VALUE_COUNT_SENT1'],axis=1,inplace=True)

            cols2 = ['CIF_ID']
            cols2.extend([str(col) + "_" + str(mnth) + "M" for col in comb1.iloc[:,1:].columns])
            comb1.columns = cols2     
            # print(comb1.head(3))
            #comb1.to_csv(self.data_dir + "funds_transfer/ft_features/" + enddate.date().isoformat() + ".csv")
            
            end = time.time()
            print("funds_transfer_features data - shape:", comb1.shape, 'duration:', end - start, "seconds")
            return comb1
        except Exception as e:
            print(e)
    
    def trans_features(self, enddate):
        """
        Generate transaction features from data
        """
        print("Start executing trans_features()")
        try:
            start = time.time()
            start_date = enddate.replace(day=1)
            d = datas2.Metadata()
            # print("fetch transactions raw data")
            #transactions = d.transaction_data(enddate)
            
            # #If using csvs: use only as a last option
            print(start_date)
            dates2 = pd.date_range(end=start_date,periods = 6,freq='M')
            print(dates2)
            tr_list = []
            for dtx in dates2:
                # print("reading:", d.date().isoformat())
                df = pd.read_csv(self.data_dir+"tran_data/"+dtx.date().isoformat()+".csv",
                            low_memory=False,converters={'CIF_ID': lambda x: str(x)})
                tr_list.append(df)
            transactions = pd.concat(tr_list, 0)
            print("concat successfull")

            def percentile(n):
                def percentile_(x):
                    return np.percentile(x, n)
                percentile_.__name__ = 'percentile_%s' % n
                return percentile_

            transactions1 = transactions.copy()
            transactions1['VAR_CRNCY_UNITS'] = transactions1['VAR_CRNCY_UNITS'].fillna(1)

            ###TRX FEATURES
            transactions1['TRAN_AMT_UGX'] = abs(transactions1['TRAN_AMT']) * transactions1['VAR_CRNCY_UNITS']
            transactions1 = transactions1.drop(['VAR_CRNCY_UNITS'],axis=1)
            transactions1['TRAN_MONTH'] = transactions1['TRAN_MONTH'].str[11:]
            
            transactions1['TRAN_AMT'] = transactions1['TRAN_AMT_UGX']
            
            transactions2 = transactions1.groupby(['CIF_ID', 'TRAN_MONTH', 'PART_TRAN_TYPE'])\
                .agg(TRAN_AMT=('TRAN_AMT', 'sum'), NUMBEROFTRANS=('FORACID', 'count'),\
                    DELIVERY_CHANNEL_UNIQUE_COUNT=('DELIVERY_CHANNEL_ID','nunique'),TRAN_AMT_90th_PERCENTILE=('TRAN_AMT',percentile(90))).reset_index()
                
            startdate_p = enddate.strftime('%Y%m')
            
            startdate_6_p = (enddate - pd.DateOffset(months=6)).strftime('%Y%m')
            # print("startdate_6_p:", startdate_6_p)
            
            startdate_3_p = (enddate - pd.DateOffset(months=3)).strftime('%Y%m')
            # print("startdate_3_p:", startdate_3_p)
            
            startdate_1_p = (enddate - pd.DateOffset(months=1)).strftime('%Y%m')
            # print("startdate_1_p:", startdate_1_p)

            trans_total = transactions2.copy()
            
            ##add missing months
            tran_dates = pd.date_range(end=start_date, periods=6, freq='M')
            tran_month = []
            for date1 in tran_dates:
                enddate_p = date1.strftime('%Y%m')
                tran_month.append(enddate_p)
            # print(1)
                
            cifs = trans_total['CIF_ID'].unique()
            tran_type = trans_total['PART_TRAN_TYPE'].unique()
            idx = pd.MultiIndex.from_product((tran_month, cifs, tran_type))
            # print(2)
            b = trans_total.set_index(
                ['TRAN_MONTH', 'CIF_ID', 'PART_TRAN_TYPE']).reindex(idx).reset_index()
            b = b.rename(columns={
                'level_0': 'TRAN_MONTH', 'level_1': 'CIF_ID', 'level_2': 'PART_TRAN_TYPE'})

            # print(3)
            cols = ['CIF_ID', 'TRAN_MONTH', 'PART_TRAN_TYPE']
            for col in cols:
                b[col] = b[col].astype(str)
                trans_total[col] = trans_total[col].astype(str)

            # print(4)
            trans_total1 = pd.merge(b[['CIF_ID', 'TRAN_MONTH', 'PART_TRAN_TYPE']], trans_total, on=[
                                    'CIF_ID', 'TRAN_MONTH', 'PART_TRAN_TYPE'], how='left')
            trans_total1 = trans_total1.fillna(0)
            
            
            # # print(8)
            ##get 6 months data
            model_data_6 = trans_total1[
                (trans_total1['TRAN_MONTH'] >= startdate_6_p) & (trans_total1['TRAN_MONTH'] < startdate_p)]
                
            cr_dr_ratio1 = pd.pivot_table(data=model_data_6,index=['CIF_ID','TRAN_MONTH'],columns='PART_TRAN_TYPE',
                            values='NUMBEROFTRANS',aggfunc='sum').reset_index().rename_axis(None, axis=1)
            
            ##mitgate infinity values
            cr_dr_ratio1['D'] = cr_dr_ratio1['D']+0.000001
            cr_dr_ratio1['C'] = cr_dr_ratio1['C']+0.000001
            cr_dr_ratio1['DR_CR_RATIO_6M'] = cr_dr_ratio1['D']/cr_dr_ratio1['C']
            
            cr_dr_ratio1 = cr_dr_ratio1.fillna(0)
            cr_dr_ratio2 = cr_dr_ratio1.groupby('CIF_ID').agg(DD_DC_MEDIAN_6M=('DR_CR_RATIO_6M','median'),DD_DC_AVG_6M=('DR_CR_RATIO_6M','mean'))  
            # Apply aggregate operations to generate additional variables
            ###Aggregations over 6 months
            trans_6_summ1 = model_data_6.groupby(['CIF_ID', 'TRAN_MONTH']). \
                agg({'TRAN_AMT': 'sum', 'NUMBEROFTRANS': 'sum'}).reset_index()
                
            trans_6_summ = trans_6_summ1.groupby(['CIF_ID']). \
                agg({'TRAN_AMT': np.count_nonzero})
            
            trans_6_summ = trans_6_summ.reset_index()
            
            trans_6_summ = trans_6_summ.rename(
                columns={'TRAN_AMT': 'NUMBER_OF_MONTHS_ACTIVE_6M'})
                
            trans_6_summ.columns = trans_6_summ.columns.str.upper()
            
            trans_6_summ = pd.merge(trans_6_summ,cr_dr_ratio2,how='inner',on='CIF_ID')
            # print(9)
            trans_6 = model_data_6.groupby(['CIF_ID', 'PART_TRAN_TYPE']). \
                agg({'TRAN_AMT': ['sum', 'median', 'max', np.count_nonzero, 'min', 'std', percentile(75)],
                    'NUMBEROFTRANS': ['sum'],'TRAN_AMT_90th_PERCENTILE':['median','mean']})
            trans_6.columns = [x1 + "_" + x2 +
                            "_6M" for x1, x2 in trans_6.columns]
            trans_6 = trans_6.reset_index()
            trans_6 = trans_6.rename(
                columns={'TRAN_AMT_count_nonzero_6M': 'NUMBER_OF_MONTHS_ACTIVE_6M'})
            trans_6.columns = trans_6.columns.str.upper()
            
            trans_6 = pd.pivot_table(trans_6, index='CIF_ID', columns='PART_TRAN_TYPE',
                                    values=['TRAN_AMT_SUM_6M', 'TRAN_AMT_MEDIAN_6M',
                                            'TRAN_AMT_MAX_6M', 'NUMBER_OF_MONTHS_ACTIVE_6M', 'TRAN_AMT_MIN_6M',
                                            'TRAN_AMT_STD_6M', 'TRAN_AMT_PERCENTILE_75_6M',
                                            'NUMBEROFTRANS_SUM_6M','TRAN_AMT_90TH_PERCENTILE_MEDIAN_6M','TRAN_AMT_90TH_PERCENTILE_MEAN_6M'], aggfunc='sum')
            trans_6.columns = [x1 + "_" + x2 for x1, x2 in trans_6.columns]
            trans_6 = trans_6.reset_index()
            trans_6.groupby('NUMBER_OF_MONTHS_ACTIVE_6M_C').size()
            # print(10)
            trans_6['AVG_TRAN_AMT_6M_C'] = trans_6['TRAN_AMT_SUM_6M_C']/6
            trans_6['AVG_TRAN_AMT_6M_D'] = trans_6['TRAN_AMT_SUM_6M_D']/6
            trans_6['AVG_NUMBEROFTRANS_6M_C'] = trans_6['NUMBEROFTRANS_SUM_6M_C']/6
            trans_6['AVG_NUMBEROFTRANS_6M_D'] = trans_6['NUMBEROFTRANS_SUM_6M_D']/6
            
            trans_6_combined = pd.merge(trans_6_summ, trans_6, how='inner', on='CIF_ID')
            
            ##get 3 months data
            model_data_3 = trans_total1[
                (trans_total1['TRAN_MONTH'] >= startdate_3_p) & (trans_total1['TRAN_MONTH'] < startdate_p)]

            # print(11)
            cr_dr_ratio1 = pd.pivot_table(data=model_data_3,index=['CIF_ID','TRAN_MONTH'],columns='PART_TRAN_TYPE',
                            values='NUMBEROFTRANS',aggfunc='sum').reset_index().rename_axis(None, axis=1)
            
            ##mitgate infinity values
            cr_dr_ratio1['D'] = cr_dr_ratio1['D']+0.000001
            cr_dr_ratio1['C'] = cr_dr_ratio1['C']+0.000001
            
            cr_dr_ratio1['DR_CR_RATIO_3M'] = cr_dr_ratio1['D']/cr_dr_ratio1['C']
            
            cr_dr_ratio1 = cr_dr_ratio1.fillna(0)
            cr_dr_ratio2 = cr_dr_ratio1.groupby('CIF_ID').agg(DD_DC_MEDIAN_3M=('DR_CR_RATIO_3M','median'),DD_DC_AVG_3M=('DR_CR_RATIO_3M','mean'))  
                
            # Apply aggregate operations to generate additional variables
            trans_3_summ1 = model_data_3.groupby(['CIF_ID', 'TRAN_MONTH']). \
                agg({'TRAN_AMT': 'sum', 'NUMBEROFTRANS': 'sum'}).reset_index()
                
            trans_3_summ = trans_3_summ1.groupby(['CIF_ID']). \
                agg({'TRAN_AMT': np.count_nonzero})
            
            trans_3_summ = trans_3_summ.reset_index()
            
            trans_3_summ = trans_3_summ.rename(
                columns={'TRAN_AMT': 'NUMBER_OF_MONTHS_ACTIVE_3M'})
                
            trans_3_summ.columns = trans_3_summ.columns.str.upper()
            # print(12)
            trans_3_summ = pd.merge(trans_3_summ,cr_dr_ratio2,how='inner',on='CIF_ID')
            
            trans_3 = model_data_3.groupby(['CIF_ID', 'PART_TRAN_TYPE']). \
                agg({'TRAN_AMT': ['sum', 'median', 'max', np.count_nonzero, 'min', 'std', percentile(75)],
                    'NUMBEROFTRANS': ['sum']})
            trans_3.columns = [x1 + "_" + x2 +
                            "_3M" for x1, x2 in trans_3.columns]
            trans_3 = trans_3.reset_index()
            trans_3 = trans_3.rename(
                columns={'TRAN_AMT_count_nonzero_3M': 'NUMBER_OF_MONTHS_ACTIVE_3M'})
            trans_3.columns = trans_3.columns.str.upper()
            
            trans_3 = pd.pivot_table(trans_3, index='CIF_ID', columns='PART_TRAN_TYPE',
                                    values=['TRAN_AMT_SUM_3M', 'TRAN_AMT_MEDIAN_3M',
                                            'TRAN_AMT_MAX_3M', 'NUMBER_OF_MONTHS_ACTIVE_3M', 'TRAN_AMT_MIN_3M',
                                            'TRAN_AMT_STD_3M', 'TRAN_AMT_PERCENTILE_75_3M',
                                            'NUMBEROFTRANS_SUM_3M'], aggfunc='sum')
            trans_3.columns = [x1 + "_" + x2 for x1, x2 in trans_3.columns]
            trans_3 = trans_3.reset_index()
            # print(13)
            trans_3['AVG_TRAN_AMT_3M_C'] = trans_3['TRAN_AMT_SUM_3M_C']/3
            trans_3['AVG_TRAN_AMT_3M_D'] = trans_3['TRAN_AMT_SUM_3M_D']/3
            trans_3['AVG_NUMBEROFTRANS_3M_C'] = trans_3['NUMBEROFTRANS_SUM_3M_C']/3
            trans_3['AVG_NUMBEROFTRANS_3M_D'] = trans_3['NUMBEROFTRANS_SUM_3M_D']/3
            
            trans_3_combined = pd.merge(trans_3_summ, trans_3, how='inner', on='CIF_ID')
            
            ##aggregate 1 month
            ##
            model_data_1 = transactions1[
                (transactions1['TRAN_MONTH'] >= startdate_1_p) & (transactions1['TRAN_MONTH'] < startdate_p)]
            # print(14)
            ##aggregate df as is
            df = model_data_1.groupby(['CIF_ID','PART_TRAN_TYPE']).agg({'TRAN_AMT': ['sum', 'median', 'max', 'min',
                                'std', percentile(75)]})
            df.columns = [x1 + "_" + x2 +
                            "_1M" for x1, x2 in df.columns]
            df = df.reset_index()
            
            df.columns = df.columns.str.upper()
            
            df_1 = pd.pivot_table(df, index='CIF_ID', columns='PART_TRAN_TYPE',
                                    values=['TRAN_AMT_SUM_1M', 'TRAN_AMT_MEDIAN_1M',
                                            'TRAN_AMT_MAX_1M', 'TRAN_AMT_MIN_1M',
                                            'TRAN_AMT_STD_1M', 'TRAN_AMT_PERCENTILE_75_1M'], aggfunc='sum')
            df_1.columns = [x1 + "_" + x2 for x1, x2 in df_1.columns]
            df_1 = df_1.reset_index()

            df_2 = model_data_1.groupby(['CIF_ID']). \
                agg({'TRAN_AMT': ['median', 'max', 'min',
                                'std', percentile(75)],'TRAN_DATE':'nunique'})
                                
            df_2.columns = [x1 + "_" + x2 +
                                    "_1M" for x1, x2 in df_2.columns]
                                    
            df_2 = df_2.reset_index()
            
            df_2.columns = df_2.columns.str.upper()
            
            df_2.rename(
                columns={'TRAN_DATE_NUNIQUE_1M':'NO_ACTIVE_DAYS_1M'}, inplace=True)
            
            df_3 = pd.merge(df_1,df_2,how='inner',on='CIF_ID')
            # print(15)
            ## 
            model_data_2 = model_data_1.groupby(['CIF_ID', 'TRAN_DATE', 'PART_TRAN_TYPE']).agg(
                TRAN_AMT=('TRAN_AMT', 'sum'), NUMBEROFTRANS=('TRAN_AMT', 'count')).reset_index()
            
            model_data_ = transactions2[
                (transactions2['TRAN_MONTH'] >= startdate_1_p) & (transactions2['TRAN_MONTH'] < startdate_p)]
            ####
            ##add missing days
            month_date = pd.to_datetime(enddate + pd.offsets.MonthEnd(-1))
            month_date1 = month_date.replace(day=1)
            
            tran_dates1 = pd.date_range(month_date1, month_date, freq='D')
            
            cifs = model_data_2['CIF_ID'].unique()
            tran_type = model_data_2['PART_TRAN_TYPE'].unique()
            idx = pd.MultiIndex.from_product((tran_dates1, cifs, tran_type))
            
            b = model_data_2.set_index(
                ['TRAN_DATE', 'CIF_ID', 'PART_TRAN_TYPE']).reindex(idx).reset_index()
            b = b.rename(columns={
                'level_0': 'TRAN_DATE', 'level_1': 'CIF_ID', 'level_2': 'PART_TRAN_TYPE'})
                
            cols = ['CIF_ID', 'TRAN_DATE', 'PART_TRAN_TYPE']
            for col in cols:
                b[col] = b[col].astype(str)
                model_data_2[col] = model_data_2[col].astype(str)
                
            model_data = pd.merge(b[['CIF_ID', 'TRAN_DATE', 'PART_TRAN_TYPE']], model_data_2, on=[
                'CIF_ID', 'TRAN_DATE', 'PART_TRAN_TYPE'], how='left')
            model_data = model_data.fillna(0)
            
            trans_1_summ1 = model_data.groupby(['CIF_ID']). \
                agg({'TRAN_AMT': ['median', 'max', 'min',
                                'std', percentile(75)]})
                                
            trans_1_summ1.columns = [x1 + "_" + x2 +
                                    "_DAILY_1M" for x1, x2 in trans_1_summ1.columns]
                                    
            trans_1_summ1 = trans_1_summ1.reset_index()
            
            trans_1_summ1.columns = trans_1_summ1.columns.str.upper()
            
            trans_1 = model_data_.groupby('CIF_ID').agg(LAST_MONTH_TOTAL_TRANS=('NUMBEROFTRANS','sum'),LAST_MONTH_TOTAL_AMT=('TRAN_AMT','sum'))
            
            trans_1 = trans_1.reset_index()
            
            trans_1_combined = pd.merge(trans_1_summ1, trans_1, how='inner', on='CIF_ID')
            trans_1_combined = pd.merge(trans_1_combined, df_3, how='inner', on='CIF_ID')
                                
            combined1 = pd.merge(trans_6_combined, trans_3_combined, how='left', on='CIF_ID')
            
            combined2 = pd.merge(combined1, trans_1_combined, how='left', on='CIF_ID')
            
            combined2 = combined2.fillna(0)
            # print(combined2.head(2))
            
            end = time.time()
            print("trans_features data - shape:", combined2.shape, 'duration:', end - start, "seconds")
            
            return combined2
            
        except Exception as e:
            print(e)

if __name__ == '__main__':
    start = time.time()
    c = TranFeatures()
    end = time.time()
    print('Total Time taken', end - start)



from multiprocess import Pool
import pandas as pd
import numpy as np
import time
import Projects.credit_scoring_202102.base.Properties as props
import Projects.credit_scoring_202102.base.custom_data_models as datas
import Projects.credit_scoring_202102.code.uganda.dataset_definitions as datas2
import Projects.credit_scoring_202102.code.uganda.tran_features as tranfeatures
import Projects.credit_scoring_202102.code.uganda.eod_features as eodfeatures
import Projects.credit_scoring_202102.code.uganda.sor as sor_features
import gc
from datetime import datetime
from dateutil.relativedelta import relativedelta
import cx_Oracle
import numpy as np

"""@Runtime- in model mode- 30 Minutes if reading from csv"""
"""@Runtime- in predict mode- 1 hour (No csvs used)"""

class FeatureMerge():
    def __init__(self):
        self.data_dir = props.uganda_data_path
    
    def feature_merge(self, enddates, data_mode):
        #enddates = pd.to_datetime("2021-11-30")
        """
        This function will merge all features and produce a dataset for prediction
        """
        try:
            for enddate in enddates:
                print(enddate)
                start = time.time()
                start_date = enddate.replace(day=1)
                d = datas2.Metadata()
                trans = tranfeatures.TranFeatures()
                sor_output = sor_features.SorFeatures()
                eod = eodfeatures.EodFeatures()
                
                accounts_data = d.demographics(enddate)
                
                print("demographics Done")
                
                maxdpdb4 = d.extract_dpd_before(enddate)
                
                print("extract_dpd_before Done")
                
                ##currency codes
                crncy = d.currency_codes()
                crncy.rename(columns={'FXD_CRNCY_CODE':'ACCT_CRNCY_CODE'},inplace=True)
                crncy['RTLIST_DATE'] = crncy['RTLIST_DATE'].astype(str)
                crncy = crncy[crncy['RTLIST_DATE']>'2016-12-31']
                crncy['RTLIST_DATE'] = pd.to_datetime(crncy['RTLIST_DATE'])
                crncy['TRAN_MONTH'] = crncy['RTLIST_DATE'].dt.strftime('%Y%m')
                
                
                loan_data = d.loan_data(enddate)
                loan_data['TRAN_MONTH'] = loan_data['ACCT_OPN_DATE'].dt.strftime('%Y%m')
                loan_data = pd.merge(loan_data,crncy[['TRAN_MONTH','VAR_CRNCY_UNITS','ACCT_CRNCY_CODE']],how='left',on=['TRAN_MONTH','ACCT_CRNCY_CODE'])
                loan_data['VAR_CRNCY_UNITS'] = loan_data['VAR_CRNCY_UNITS'].fillna(1)
                loan_data['DISBURSED_AMT_UGX'] = loan_data['SANCT_LIM']*loan_data['VAR_CRNCY_UNITS']
                loan_data = loan_data.drop(['VAR_CRNCY_UNITS'],axis=1)
                
                loan_data['DISBURSED_AMT'] = loan_data['DISBURSED_AMT_UGX']
                
                sor_loans = loan_data[loan_data['ACCT_OPN_DATE']<start_date]
                
                print("loan_data Done")
                
                # print("trans_features start")
                trans2 = trans.trans_features(enddate)
                
                print("trans_features Done")
                
                fts = trans.funds_trans(enddate)
                
                print("funds_trans Done")
                
                eod_data = eod.eod_features(enddate)
                
                print("eod_features Done")
                
                sor = sor_output.sor_assign(
                    enddate, accounts_data, trans2, eod_data, sor_loans)
                    
                print("sor_output Done")
                
                # print("step 2")
                #writeoffs = d.written_off(enddate)
                
                accounts1 = accounts_data.copy()
                loans = loan_data.copy()
                
                transactions1 = trans2.copy()
                eod_balance1 = eod_data.copy()
                
                ##demographics
                accounts1['RELATIONSHIPOPENINGDATE'] = pd.to_datetime(accounts1['RELATIONSHIPOPENINGDATE'])
                accounts1['CUST_DOB'] = pd.to_datetime(accounts1['CUST_DOB'])
                accounts1['CUST_TENURE_MONTHS'] = (
                    start_date-pd.to_datetime(accounts1['RELATIONSHIPOPENINGDATE'])).astype('<m8[M]')
                # print("step 2.1")
                accounts1['AGE'] = (
                    start_date-pd.to_datetime(accounts1['CUST_DOB'])).astype('<m8[Y]')
                # print("step 2.2")
                accounts2 = accounts1[[
                    'CIF_ID', 'PRIMARY_SOL_ID', 'SEGMENTATION_CLASS', 'GENDER', 'STATE', 'CUST_TENURE_MONTHS', 'AGE','MANAGER']]
                    
                cat_cols = ['SEGMENTATION_CLASS',
                            'PRIMARY_SOL_ID', 'GENDER', 'STATE']
                # print("step 3")
                for cols in cat_cols:
                    accounts2[cols] = accounts2[cols].astype('category')
                    accounts2[cols] = accounts2[cols].cat.add_categories(
                        'Unknown')
                    accounts2[cols].fillna('Unknown', inplace=True)
                    accounts2[cols] = accounts2[cols].astype('category')
                # print("step 4")
                cols = accounts2.columns
                for element in cols:
                    if element not in cat_cols:
                        accounts2[element].fillna(0, inplace=True)
                    else:
                        pass
                
                ##Loan features
                loans['ACCT_OPN_DATE'] = pd.to_datetime(loans['ACCT_OPN_DATE'])
                loans1 = loans[loans['ACCT_OPN_DATE']<start_date]
                loans1['STATUS'] = np.where(
                    loans1['ACCT_CLS_DATE'] < start_date, 'CLOSED_LOANS', 'OPEN_LOANS')
                    
                loans1 = loans1.groupby(['CIF_ID', 'STATUS']).agg(count_loans=(
                    'STATUS', 'count'), total_lim=('DISBURSED_AMT', 'sum')).reset_index()
                # print("step 5")
                loans2 = pd.pivot_table(loans1, values=[
                                        'count_loans', 'total_lim'], index='CIF_ID', columns='STATUS', aggfunc='sum').fillna(0)
                loans2.columns = [x1 + "_" + x2 for x1, x2 in loans2.columns]
                loans2 = loans2.reset_index()
                loans2['TOTAL_LOAN_AMT_DISB'] = loans2['total_lim_CLOSED_LOANS'] + \
                    loans2['total_lim_OPEN_LOANS']
                loans2.rename(columns={'count_loans_CLOSED_LOANS': 'CLOSED_LOANS',
                                       'count_loans_OPEN_LOANS': 'OPEN_LOANS'}, inplace=True)
                loans2['AVG_AMT_DISB'] = loans2['TOTAL_LOAN_AMT_DISB'] / \
                    (loans2['CLOSED_LOANS'] + loans2['OPEN_LOANS'])
                loans2['REPAYMENT_RATIO'] = loans2['total_lim_CLOSED_LOANS'] / \
                    loans2['TOTAL_LOAN_AMT_DISB']
                loans2['OUTSTANDING_RATIO'] = loans2['total_lim_OPEN_LOANS'] / \
                    loans2['TOTAL_LOAN_AMT_DISB']
                loans2 = loans2[['CIF_ID', 'CLOSED_LOANS', 'OPEN_LOANS',
                                 'AVG_AMT_DISB', 'REPAYMENT_RATIO', 'OUTSTANDING_RATIO']]
                loans2 = loans2.fillna(0)
                # print("step 6")
                ###pick personal banking loans
                loan_data2 = loans[(loans['ACCT_OPN_DATE'] >= start_date) & (loans['ACCT_OPN_DATE']<=enddate)]
                loan_data3 = loan_data2[loan_data2['SCHM_CODE'].isin (
                    ['LA502','LA508','LA523','LA563'])]
                    
                # print("step 7")
                ##merge_features
                merge_features = pd.merge(
                    transactions1, eod_balance1, how='outer', on='CIF_ID')
                    
                merge_features = pd.merge(
                    merge_features, loans2, how='outer', on='CIF_ID')
                
                merge_features = pd.merge(
                    merge_features, fts, how='outer', on='CIF_ID')
                print("last merge before mode",merge_features.shape)  
                
                # print("step 8")
                if data_mode == 'M':
                    maxdpdafter = d.extract_dpd_after(enddate)
                    print("extract_dpd_after Done")
                    
                    dpd = pd.merge(maxdpdb4, maxdpdafter, how='outer',on='CIF_ID')
                    dpd = dpd.fillna(0)
                    
                    percolate = dpd[dpd['CIF_ID'].isin(loan_data3.CIF_ID)]
                    percolate = percolate.reset_index(drop=True)
                    print("percolate",percolate.shape) 
                    
                    merge_features1 = pd.merge(
                        percolate, merge_features, how='inner', on='CIF_ID')
                    
                    merge_features2 = pd.merge(
                        merge_features1, accounts2, how='inner', on='CIF_ID')
                        
                    merge_features2 = pd.merge(
                        merge_features2, sor, how='left', on='CIF_ID')
                    
                    merge_features2['LOANS_TO_TENURE'] = (
                    merge_features2['CLOSED_LOANS'] + merge_features2['OPEN_LOANS'])/merge_features2['CUST_TENURE_MONTHS']
                    
                    merge_features3 = merge_features2.round(3)
                    
                    merge_features3.to_csv(self.data_dir+"model_features/"+enddate.date().isoformat()+"_v1.csv",
                                           index=False)
                    print("final merge: ", merge_features3.shape)
                    end = time.time()
                    print ("Time Taken", end - start, "seconds")
                else:
                    merge_features1 = pd.merge(
                        accounts2, merge_features, how='left', on='CIF_ID')
                        
                    merge_features2 = pd.merge(
                        merge_features1, maxdpdb4, how='left', on='CIF_ID')
                        
                    merge_features2 = pd.merge(
                        merge_features2, sor, how='left', on='CIF_ID')
                        
                    # print("demo1: ", accounts2.shape)
                    
                    merge_features2['LOANS_TO_TENURE'] = (
                    merge_features2['CLOSED_LOANS'] + merge_features2['OPEN_LOANS'])/merge_features2['CUST_TENURE_MONTHS']
                    
                    merge_features3 = merge_features2.round(3)
                    merge_features3.to_csv(self.data_dir+"predict_features/"+enddate.date().isoformat()+"_v1.csv", index=False)
                    
                    end = time.time()
                    print('merge_features - shape:', merge_features3.shape,
                          "duration:", end - start, " @date:", enddate.date().isoformat())
            
                    
        except Exception as e:
            print(e)

if __name__ == '__main__':
    data_extract = FeatureMerge()
    start = time.time()
    enddates = pd.date_range(start='2021-01-31', periods=12, freq='M')
    #enddates = pd.date_range(start='2022-02-28', periods=1, freq='M')
    print(enddates)
    data_extract.feature_merge(enddates, 'M')
    end = time.time()
    print('Total Time taken', end-start)




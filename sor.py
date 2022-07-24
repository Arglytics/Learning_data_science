import pandas as pd
import re
import numpy as np
from Projects.credit_scoring_202102.base import Properties
import Projects.credit_scoring_202102.base.custom_data_models as datas
import Projects.credit_scoring_202102.base.Properties as props
import time
from datetime import timedelta, date
import datetime
import gc
from multiprocessing import Pool

pd.set_option('display.float_format',lambda x:'%.5f'%x)


class SorFeatures():
    def __init__(self):
        self.data_dir = props.uganda_data_path
    
    def zero_pad(self,v):
        return "0"+str(v) if v < 10 else str(v)

    def sor_assign(self, enddate, demo, trans_features, eod_data, loans):
        try:
            # print(1)
            start = time.time()
                
            accounts = demo

            loan_data = loans
            
            trans_features1 = trans_features

            eod_data1 = eod_data
            # print(eod_data1.head())

            start_date=enddate.replace(day=1)
            # print(2)

            accounts['RELATIONSHIPOPENINGDATE'] = pd.to_datetime(accounts['RELATIONSHIPOPENINGDATE'])
            accounts['MONTHS_WITHBANK'] = ((start_date- pd.DateOffset(days=1))-accounts['RELATIONSHIPOPENINGDATE']).astype('<m8[M]')
            # print(2.1)
            #1 MONTH PREPROCESSING
            trans_data1 = trans_features1[['CIF_ID','AVG_TRAN_AMT_3M_C','AVG_TRAN_AMT_6M_C','TRAN_AMT_SUM_1M_C','AVG_NUMBEROFTRANS_3M_C',
                                        'AVG_NUMBEROFTRANS_3M_D','AVG_NUMBEROFTRANS_6M_C','AVG_NUMBEROFTRANS_6M_D',
                                        'LAST_MONTH_TOTAL_TRANS']]
            cols = ['CIF_ID','AVG_CR_AMT_L3M','AVG_CR_AMT_L6M','CR_AMT_LAST_MNTH','AVG_CR_TRANX_L3M','AVG_DR_TRANX_L3M',
                'AVG_CR_TRANX_L6M','AVG_DR_TRANX_L6M','TRANX_LAST_MONTH']
            trans_data1.columns = cols
            # print(2.2)
            trans_data1['CIF_ID'] = trans_data1['CIF_ID'].astype(str)
            
            ##avg bal 6 and 3
            eod_data2 = eod_data1[['CIF_ID','AVG_BALANCE_MEAN_6M','AVG_BALANCE_MEAN_3M']]
            cols = ['CIF_ID','AVG_EOD_BAL_L6M','AVG_EOD_BAL_L3M']
            eod_data2.columns = cols
            # print(3)
            eod_data2['CIF_ID'] = eod_data2['CIF_ID'].astype(str)
            alldata=pd.merge(trans_data1,eod_data2,on=['CIF_ID'],how='outer')

            loans1 = loan_data.groupby(['CIF_ID']).agg(NUM_OF_LOANS_L3YRS=(
                'FORACID', 'count')).reset_index()
            loans1['CIF_ID'] = loans1['CIF_ID'].astype(str)

            alldata1=pd.merge(alldata,loans1,on='CIF_ID',how='outer')

            # print(accounts.dtypes)
            # print(alldata1.dtypes)
            alldata2 = pd.merge(accounts[['CIF_ID','MONTHS_WITHBANK']],alldata1,on='CIF_ID',how='left')
            
            alldata2=alldata2.fillna(0)

            # print(4)
            
            alldata2['NUM_OF_LOANS_L3YRS'] = alldata2['NUM_OF_LOANS_L3YRS'].astype(int)
            alldata2['TRANX_LAST_MONTH'] = alldata2['TRANX_LAST_MONTH'].astype(int)
            
            def weight_sor(bank_month):
                if bank_month<=1.0:
                    return 1
                elif (bank_month>1.0 and bank_month<=3.0):
                    return 2
                elif (bank_month>3.0 and bank_month<=6.0):
                    return 3
                elif (bank_month>6.0 and bank_month<=9.0):
                    return 4
                elif (bank_month>9.0 and bank_month<=12.0):
                    return 5
                elif (bank_month>12.0 and bank_month<=18.0):
                    return 6
                elif (bank_month>18.0 and bank_month<=36.0):
                    return 7
                elif bank_month>36.0:
                    return 8
                    
            alldata2['MONTHS_WITHBANK_sor'] = alldata2.apply(lambda x: weight_sor(x.MONTHS_WITHBANK), axis=1)
            alldata2['MONTHS_WITHBANK_sor_weight']=alldata2['MONTHS_WITHBANK_sor']*0.125
            
            def weight_sor(cr_month):
                if cr_month<=0.0:
                    return 1
                elif (cr_month>0.0 and cr_month<=15000.0):
                    return 2
                elif (cr_month>15000.0 and cr_month<=30000.0):
                    return 3
                elif (cr_month>30000.0 and cr_month<=150000.0):
                    return 4
                elif (cr_month>150000.0 and cr_month<=300000.0):
                    return 5
                elif (cr_month>300000.0 and cr_month<=1500000.0):
                    return 6
                elif (cr_month>1500000.0 and cr_month<=3000000.0):
                    return 7
                elif cr_month>3000000.0:
                    return 8
                    
            alldata2['CR_AMT_LAST_MNTH_sor'] = alldata2.apply(lambda x: weight_sor(x.CR_AMT_LAST_MNTH), axis=1)
            alldata2['CR_AMT_LAST_MNTH_sor_weight'] = alldata2['CR_AMT_LAST_MNTH_sor']*0.075
            alldata2['AVG_CR_AMT_L3M_sor'] = alldata2.apply(lambda x: weight_sor(x.AVG_CR_AMT_L3M), axis=1)
            alldata2['AVG_CR_AMT_L3M_sor_weight'] = alldata2['AVG_CR_AMT_L3M_sor']*0.05
            alldata2['AVG_CR_AMT_L6M_sor'] = alldata2.apply(lambda x: weight_sor(x.AVG_CR_AMT_L6M), axis=1)
            alldata2['AVG_CR_AMT_L6M_sor_weight'] = alldata2['AVG_CR_AMT_L6M_sor']*0.025
            
            def weight_sor(loans):
                if loans==0:
                    return 1
                elif loans==1:
                    return 2
                elif loans==2:
                    return 3
                elif loans==3:
                    return 4
                elif loans==4:
                    return 5
                elif loans==5:
                    return 6
                elif loans>5 and loans<=10:
                    return 7
                elif loans>10:
                    return 8
                    
            alldata2['NUM_OF_LOANS_L3YRS_sor'] = alldata2.apply(lambda x: weight_sor(x.NUM_OF_LOANS_L3YRS), axis=1)
            alldata2['NUM_OF_LOANS_L3YRS_sor_weight'] = alldata2['NUM_OF_LOANS_L3YRS_sor']*0.125
            # print(5)
            def weight_sor(eod_bal):
                if eod_bal<=0.0:
                    return 1
                elif (eod_bal>1500.0 and eod_bal<=3000.0):
                    return 2
                elif (eod_bal>3000.0 and eod_bal<=15000.0):
                    return 3
                elif (eod_bal>15000.0 and eod_bal<=30000.0):
                    return 4
                elif (eod_bal>30000.0 and eod_bal<=150000.0):
                    return 5
                elif (eod_bal>150000.0 and eod_bal<=300000.0):
                    return 6
                elif (eod_bal>300000.0 and eod_bal<=1500000.0):
                    return 7
                elif eod_bal>1500000.0:
                    return 8
                    
            alldata2['AVG_EOD_BAL_L3M_sor'] = alldata2.apply(lambda x: weight_sor(x.AVG_EOD_BAL_L3M), axis=1)
            alldata2['AVG_EOD_BAL_L3M_sor_weight'] = alldata2['AVG_EOD_BAL_L3M_sor']*0.05
            alldata2['AVG_EOD_BAL_L6M_sor'] = alldata2.apply(lambda x: weight_sor(x.AVG_EOD_BAL_L6M), axis=1)
            alldata2['AVG_EOD_BAL_L6M_sor_weight'] = alldata2['AVG_EOD_BAL_L6M_sor']*0.05
            
            def weight_sor(trx_last_month):
                if trx_last_month==0:
                    return 1
                elif trx_last_month==1:
                    return 2
                elif trx_last_month==2:
                    return 3
                elif (trx_last_month>2 and trx_last_month<=4):
                    return 4
                elif (trx_last_month>4 and trx_last_month<=6):
                    return 5
                elif (trx_last_month>6 and trx_last_month<=9):
                    return 6
                elif (trx_last_month>9 and trx_last_month<=12):
                    return 7
                elif trx_last_month>12:
                    return 8
                    
            alldata2['TRANX_LAST_MONTH_sor'] = alldata2.apply(lambda x: weight_sor(x.TRANX_LAST_MONTH), axis=1)
            alldata2['TRANX_LAST_MONTH_sor_weight'] = alldata2['TRANX_LAST_MONTH_sor']*0.15
            
            def weight_sor(cr_trnx):
                if cr_trnx<=0.0:
                    return 1
                elif (cr_trnx>0.0 and cr_trnx<=0.3):
                    return 2
                elif (cr_trnx>0.3 and cr_trnx<=0.6):
                    return 3
                elif (cr_trnx>0.6 and cr_trnx<=1.0):
                    return 4
                elif (cr_trnx>1.0 and cr_trnx<=1.2):
                    return 5
                elif (cr_trnx>1.2 and cr_trnx<=1.5):
                    return 6
                elif (cr_trnx>1.5 and cr_trnx<=2.0):
                    return 7
                elif cr_trnx>2:
                    return 8
                    
            alldata2['AVG_CR_TRANX_L3M_sor'] = alldata2.apply(lambda x: weight_sor(x.AVG_CR_TRANX_L3M), axis=1)
            alldata2['AVG_CR_TRANX_L3M_sor_weight'] = alldata2['AVG_CR_TRANX_L3M_sor']*0.1
            alldata2['AVG_CR_TRANX_L6M_sor'] = alldata2.apply(lambda x: weight_sor(x.AVG_CR_TRANX_L6M), axis=1)
            alldata2['AVG_CR_TRANX_L6M_sor_weight'] = alldata2['AVG_CR_TRANX_L6M_sor']*0.1
            
            def weight_sor(dr_trnx):
                if dr_trnx<=0.0:
                    return 1
                elif (dr_trnx>0.0 and dr_trnx<=1.0):
                    return 2
                elif (dr_trnx>1.0 and dr_trnx<=1.5):
                    return 3
                elif (dr_trnx>1.5 and dr_trnx<=3.0):
                    return 4
                elif (dr_trnx>3.0 and dr_trnx<=5.0):
                    return 5
                elif (dr_trnx>5.0 and dr_trnx<=7.0):
                    return 6
                elif (dr_trnx>7.0 and dr_trnx<=10.0):
                    return 7
                elif dr_trnx>10.0:
                    return 8
                    
            alldata2['AVG_DR_TRANX_L3M_sor'] = alldata2.apply(lambda x: weight_sor(x.AVG_DR_TRANX_L3M), axis=1)
            alldata2['AVG_DR_TRANX_L3M_sor_weight'] = alldata2['AVG_DR_TRANX_L3M_sor']*0.075
            alldata2['AVG_DR_TRANX_L6M_sor'] = alldata2.apply(lambda x: weight_sor(x.AVG_DR_TRANX_L6M), axis=1)
            alldata2['AVG_DR_TRANX_L6M_sor_weight'] = alldata2['AVG_DR_TRANX_L6M_sor']*0.075
            
            alldata2['SOR'] = alldata2['MONTHS_WITHBANK_sor_weight']+alldata2['CR_AMT_LAST_MNTH_sor_weight'] +\
                            alldata2['AVG_CR_AMT_L3M_sor_weight'] +\
                            alldata2['AVG_CR_AMT_L6M_sor_weight'] + alldata2['NUM_OF_LOANS_L3YRS_sor_weight'] +\
                            alldata2['AVG_EOD_BAL_L3M_sor_weight'] + alldata2['AVG_EOD_BAL_L6M_sor_weight'] +\
                            alldata2['TRANX_LAST_MONTH_sor_weight'] +\
                            alldata2['AVG_CR_TRANX_L3M_sor_weight'] + alldata2['AVG_CR_TRANX_L6M_sor_weight'] +\
                            alldata2['AVG_DR_TRANX_L3M_sor_weight'] + alldata2['AVG_DR_TRANX_L6M_sor_weight'] 
                            
            alldata2['SOR'] = (alldata2['SOR'].round()).astype(int)
            #alldata3=alldata2[cols+['FINAL_SOR']]
            
            alldata3 = alldata2[['CIF_ID', 'MONTHS_WITHBANK','CR_AMT_LAST_MNTH', 'AVG_CR_AMT_L3M', 'AVG_CR_AMT_L6M', 'NUM_OF_LOANS_L3YRS', 
                            'AVG_EOD_BAL_L3M', 'AVG_EOD_BAL_L6M','TRANX_LAST_MONTH' ,'AVG_CR_TRANX_L3M','AVG_DR_TRANX_L3M',
                            'AVG_CR_TRANX_L6M', 'AVG_DR_TRANX_L6M','SOR']]
            alldata4 = alldata3[['CIF_ID','SOR']]
            
            end = time.time()
            print('alldata4 - shape:', alldata4.shape,
                  "duration:", end - start)
            return alldata4
            #gc.collect()
            success = True
            
        except Exception as e:
            print(e)

if __name__ == '__main__':
    start=time.time()
    c = SorFeatures()
    




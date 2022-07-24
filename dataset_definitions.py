
import pandas as pd
import numpy as np
import time
from Projects.credit_scoring_202102.base.Properties import CustomFormatters
import Projects.credit_scoring_202102.base.Properties as props
import gc
from datetime import datetime
from dateutil.relativedelta import relativedelta
pd.options.mode.chained_assignment = None
from datetime import date, timedelta
from multiprocess import Pool

"""@Runtime- when reading from DB 30 minutes"""

class Metadata:
        def __init__(self):
                self.data_dir = props.uganda_data_path
                
                self.c = props.ProcessDataDbToFile()
                self.conn = self.c.get_db_conn()
                self.conn_ebods = self.c.get_db_conn_ebods()

        def demographics(self, enddate):
                try:
                        start = time.time()
                        start_date = enddate.replace(day=1)
                        demo_query = f"""
                        SELECT ORGKEY AS CIF_ID,CUST_DOB,RELATIONSHIPOPENINGDATE,occupation,primary_sol_id,segmentation_class,
                                        NAT_ID_CARD_NUM,GENDER,subsegment,city,state,manager
                                        FROM UGEDW.STG_ACCOUNTS
                                        WHERE BANK_ID='56' and PRIMARY_SOL_ID NOT IN ('888','777')
                                        AND RELATIONSHIPOPENINGDATE < '{start_date.strftime('%d-%b-%y').upper()}'
                                        AND ORGKEY IN
                                        (SELECT CIF_ID FROM UGEDW.UG_GAM
                                        WHERE SCHM_TYPE IN ('CAA','SBA') AND BANK_ID = '56' 
                                        and acct_opn_date < '{start_date.strftime('%d-%b-%y').upper()}'
                                        and acct_cls_flg = 'N')
                                        """
                        
                        # demo = pd.read_sql(demo_query,conn)
                        df = []
                        for chunk in pd.read_sql(demo_query, self.conn_ebods, chunksize=10000):
                                df.append(chunk)
                        demo = pd.concat(df, ignore_index=True)
                        demo = demo.drop_duplicates()
                        # demo.to_csv(
                        #         self.data_dir + "others1/demographics/" + "demographics.csv",
                        #                  index=False)
                        end = time.time()
                        print('demographics data - shape:', demo.shape,
                              "duration:", end - start, "@date:", enddate.date().isoformat())
                        return demo
                
                except Exception as e:
                        print("demographics:",e)
                        raise e

        def written_off(self, enddate):
                try:
                        start_date = enddate.replace(day=1)
                        start = time.time()
                        query = f"""SELECT G.SOL_ID,
                                        G.SCHM_CODE,
                                        G.CIF_ID,
                                        G.FORACID ACCT_NUM,
                                        TO_CHAR (ACCT_OPN_DATE, 'DD-Mon-YYYY') DISB_DATE,
                                        TO_CHAR (M.CHRGE_OFF_DATE, 'DD-Mon-YYYY') CHRGE_OFF_DATE,
                                        G.ACCT_CRNCY_CODE CURRENCY,
                                        g.SANCT_LIM,
                                        M.CHRGE_OFF_PRINCIPAL,
                                        M.INT_SUSPENSE_AMT,
                                        M.PENAL_INT_SUSPENSE_AMT,
                                        (M.CHRGE_OFF_PRINCIPAL + M.INT_SUSPENSE_AMT + M.PENAL_INT_SUSPENSE_AMT) WRITEOFF_AMT,
                                        E.DPD_CNTR DPD
                                        FROM UGEDW.UG_GAM G, UGEDW.STG_COT M, UGEDW.STG_GAC E,UGEDW.STG_ACCOUNTS h
                                        WHERE G.ACID = M.ACID
                                        AND G.SCHM_TYPE = 'LAA' 
                                        and g.cif_id=h.orgkey
                                        AND G.BANK_ID = '56'
                                        AND E.BANK_ID = '56'
                                        AND E.ACID = G.ACID
                                        AND E.CHRGE_OFF_FLG = 'Y'
                                        and M.CHRGE_OFF_DATE<'{start_date.strftime('%d-%b-%y').upper()}'
                                                        """
                        # writeoffs = pd.read_sql(query,conn)
                        df = []
                        for chunk in pd.read_sql(query, self.conn_ebods, chunksize=10000):
                                df.append(chunk)
                        writeoffs = pd.concat(df, ignore_index=True)
                        # writeoffs.to_csv(self.data_dir + "others1/writeoffs/" + "writeoffs.csv",
                        #         index=False)
                        end = time.time()
                        print('writeoffs data - shape:', writeoffs.shape,
                              "duration:", end - start)
                        return writeoffs
                except Exception as e:
                        print("writeoffs:",e)
                        raise e

        def transaction_data(self, enddate):
                try:
                        total_trans = []
                        cols = ['FORACID', 'TRAN_DATE', 'TRAN_PARTICULAR', 'TRAN_CRNCY_CODE', 'DELIVERY_CHANNEL_ID', 'TRAN_AMT', 'PART_TRAN_TYPE', 'TRAN_AMT_RWF', 'CIF_ID']
                        df2 = []
                        start = time.time()
                        #months_i = list(range(1, 13))
                        for i in range(1, 7):
                        #def run_months(i):
                                #date = datetime.now()
                                start = time.time()
                                date1 = pd.to_datetime(enddate + pd.offsets.MonthEnd(-i))
                                year = date1.strftime('%Y')
                                month = date1.strftime('%m')
                                gl = (f"""{str('STATS_SUB_P')}{str(year)}{str(month)}""")
                                ym = (f"""{str(year)}{str(month)}""")
                                gl_table = {'gl': gl, 'year': year, 'month': month, 'ym': ym}
                                print('trx:', gl)
                                trans_query=f"""
                                SELECT
                                /*+ PARALLEL */
                                S.FORACID, S.TRAN_DATE, S.TRAN_PARTICULAR,S.TRAN_CRNCY_CODE,S.DELIVERY_CHANNEL_ID,
                                S.TRAN_AMT, S.PART_TRAN_TYPE,R.VAR_CRNCY_UNITS,
                                CIF_ID 
                                FROM
                                (SELECT 
                                G.CIF_ID,G.FORACID, TRAN_DATE, TRAN_PARTICULAR,TRAN_CRNCY_CODE,DELIVERY_CHANNEL_ID,
                                TRAN_AMT, PART_TRAN_TYPE
                                FROM ugedw.{gl_table['gl']} A, UGEDW.UG_GAM G
                                where G.BANK_ID='56'
                                and A.BANK_ID='56'
                                --and A.FORACID = '4014111917579'
                                -- AND g.CIF_ID in ('50620144656') 
                                AND A.ACID=G.ACID
                                AND (
                                TRAN_NATURE IS NULL OR (TRIM(TRAN_NATURE) not in ('INTERPERSONAL TRANSFERS','INTERPERSONAL TRANSFERS2'))
                                )
                                and (a.TRAN_PARTICULAR IS NULL
                                                        or (upper(substr(a.tran_particular, 1, 6)) != 'UNPAID'
                                                        and upper(a.TRAN_PARTICULAR) not like '%/REV/%'
                                                        and upper(a.TRAN_PARTICULAR) not like '%LEDGER FEE%'
                                                        and upper(a.TRAN_PARTICULAR) not like '%REFUND%'
                                                        and upper(substr(a.tran_particular, 1, 6)) != 'REVERS'
                                                        and upper(substr(a.tran_particular, 1, 6)) != 'AC XFR'
                                                        and upper(substr(a.tran_particular, 1, 7))!= 'UN PAID'
                                                        and upper(a.tran_particular) not like '%DISBURSEMENT%CREDIT%'
                                                        and UPPER(a.TRAN_PARTICULAR) NOT LIKE '%CHARGE%'
                                                        and UPPER(SUBSTR(a.TRAN_PARTICULAR, 1, 3)) != 'REV'
                                                        and UPPER(SUBSTR(a.TRAN_PARTICULAR, 1, 3)) != 'RVS'))
                                and (a.TRAN_RMKS IS NULL OR  UPPER(a.TRAN_RMKS) NOT LIKE '%OD DISB%')
                                AND MODE_OF_OPER_CODE='SELF'
                                AND G.SCHM_TYPE IN ('CAA','SBA'))S
                                left join
                                        (
                                        select 
                                        LAST_DAY(rtlist_date) rtlist_date,ROUND(AVG(VAR_CRNCY_UNITS),2) VAR_CRNCY_UNITS,FXD_CRNCY_CODE 
                                        from ugedw.curr_rate_sub
                                        where bank_id='56'
                                        GROUP BY BANK_ID, (LAST_DAY(rtlist_date)), LAST_DAY(rtlist_date),
                                        FXD_CRNCY_CODE
                                        ) R 
                                        ON RTLIST_DATE='{date1.strftime('%d-%b-%y')}' and FXD_CRNCY_CODE=TRAN_CRNCY_CODE
                                
                                """
                                # print(trans_query)
                                # exit()
                                # trans_list = pd.read_sql(trans_query, conn)
                                # print(i)
                                df = []
                                for chunk in pd.read_sql(trans_query, self.conn_ebods, chunksize=10000):
                                        df.append(chunk)

                                if len(df)!=0:
                                        trans_list = pd.concat(df, ignore_index=True)
                                else:
                                        trans_list = pd.DataFrame(columns=cols)
                                trans_list['TRAN_MONTH'] = gl_table['gl']
                                trans_list.to_csv(self.data_dir + "tran_data/"+date1.date().isoformat()+".csv", index=False)
                                total_trans.append(trans_list)
                                end = time.time()
                                print('raw time taken',end-start)
                                # return trans_list

                        trans_df = pd.concat(total_trans)
                        
                        end = time.time()
                        print('trans data - shape:', trans_df.shape,
                              "duration:", end - start)

                        return trans_df
                
                except Exception as e:
                        print("transaction_data:", e)
                        trans_df = pd.DataFrame()
                        return trans_df

        def loan_data(self, enddate):
                try:
                        start = time.time()
                        date1 = pd.to_datetime(enddate + pd.offsets.MonthEnd(-1))
                        start_date = enddate.replace(day=1)
                        loan_start_date = ((enddate) - pd.DateOffset(years=1)).replace(day=1)
                        print("FROM: ", loan_start_date, " TO: ", enddate)

                        query = f"""select cif_id,schm_code,foracid, acct_opn_date, acct_cls_date,ACCT_CRNCY_CODE,
                                        acct_cls_flg, sanct_lim,VAR_CRNCY_UNITS
                                from (
                                (select 
                                cif_id,schm_code,foracid, acct_opn_date, acct_cls_date,ACCT_CRNCY_CODE,
                                acct_cls_flg, sanct_lim
                                from ugedw.ug_gam
                                where schm_type='LAA'
                                --AND CIF_ID in ('50620450557','50620862059') 
                                --and foracid = '4004511865667'
                                and bank_id='56' 
                                and acct_opn_date >= '{loan_start_date.strftime('%d-%b-%y').upper()}'
                                and acct_opn_date <= '{enddate.strftime('%d-%b-%y').upper()}'
                                )
                                )S
                                left join
                                (
                                select 
                                LAST_DAY(rtlist_date) rtlist_date,ROUND(AVG(VAR_CRNCY_UNITS),2) VAR_CRNCY_UNITS,FXD_CRNCY_CODE 
                                from ugedw.curr_rate_sub
                                where bank_id='56'
                                GROUP BY BANK_ID, (LAST_DAY(rtlist_date)), LAST_DAY(rtlist_date),
                                FXD_CRNCY_CODE
                                ) R 
                                ON RTLIST_DATE='{date1.strftime('%d-%b-%y')}' and FXD_CRNCY_CODE=ACCT_CRNCY_CODE
                                """

                        loan_data = pd.read_sql(query,self.conn_ebods)
                        
                        end = time.time()
                        print('loan_data - shape:', loan_data.shape,
                              "duration:", end - start, "@enddate:", enddate)
                        return loan_data
                
                except Exception as e:
                        print("loan_data:", e)

        def eod_data(self, enddate):
                try:
                        eod_data = []
                        start = time.time()
                        #months_i = list(range(1, 13))
                        for i in range(1, 13):
                        #def run_months(i):
                                start = time.time()
                                #date = datetime.now()
                                date1 = pd.to_datetime(enddate + pd.offsets.MonthEnd(-i))
                                year = date1.strftime('%Y')
                                month = date1.strftime('%m')
                                gl = (f"""{str('GLDEV_P')}{str(year)}{str(month)}""")
                                ym = (f"""{str(year)}{str(month)}""")
                                gl_table = {'gl': gl, 'year': year, 'month': month, 'ym': ym}
                                print('gl:', gl)
                                eod_query = f"""
                                        SELECT CIF_ID,avg_balance FROM 
                                        (
                                        select CUST_ID,
                                        ROUND(avg(BALANCE*DECODE(ACCT_CRNCY_CODE,'UGX',1,R.VAR_CRNCY_UNITS)),2) avg_balance
                                        from
                                        (select CUST_ID ,ACCT_CRNCY_CODE,
                                                DAY01, DAY02, DAY03, DAY04, DAY05, DAY06, DAY07, DAY08, 
                                                DAY09, DAY10, DAY11, DAY12, DAY13, DAY14, DAY15, DAY16,
                                                DAY17, DAY18, DAY19, DAY20, DAY21, DAY22, DAY23, DAY24, 
                                                DAY25, DAY26, DAY27, DAY28, DAY29, DAY30, DAY31
                                                from ugedw.{gl_table['gl']}
                                                where schm_type in ('CAA','SBA')
                                                --and cust_id='01129269'
                                                and bank_id='56'
                                                and cust_id is not null
                                                and ACCT_CLS_FLG ='N')
                                                UNPIVOT(
                                                balance  -- unpivot_clause
                                                FOR dayno --  unpivot_for_clause
                                                IN ( -- unpivot_in_clause
                                                DAY01, DAY02, DAY03, DAY04, DAY05, DAY06, DAY07, DAY08, 
                                                DAY09, DAY10, DAY11, DAY12, DAY13, DAY14, DAY15, DAY16,
                                                DAY17, DAY18, DAY19, DAY20, DAY21, DAY22, DAY23, DAY24, 
                                                DAY25, DAY26, DAY27, DAY28, DAY29, DAY30, DAY31)
                                                )
                                                left join
                                                (
                                                select 
                                                LAST_DAY(rtlist_date) rtlist_date,ROUND(AVG(VAR_CRNCY_UNITS),2) VAR_CRNCY_UNITS,FXD_CRNCY_CODE 
                                                from ugedw.curr_rate_sub
                                                where bank_id='56'
                                                             GROUP BY BANK_ID, (LAST_DAY(rtlist_date)), LAST_DAY(rtlist_date),
                                                FXD_CRNCY_CODE
                                                ) R 
                                                ON RTLIST_DATE='{date1.strftime('%d-%b-%y')}' and FXD_CRNCY_CODE=ACCT_CRNCY_CODE
                                                group by CUST_ID
                                                ) BAL
                                                LEFT JOIN
                                                (select distinct cif_id, cust_id from 
                                                UGEDW.UG_GAM G 
                                                where bank_id = '56'
                                                --AND g.CIF_ID in ('50620450557','50620862059') 
                                                and cust_id is not null
                                                and schm_type in ('CAA','SBA')
                                                ) G
                                                ON BAL.CUST_ID=G.CUST_ID"""
                                # eod_list = pd.read_sql(eod_query, conn)
                                #print(eod_query)
                                df = []
                                for chunk in pd.read_sql(eod_query, self.conn_ebods, chunksize=10000):
                                        df.append(chunk)
                                eod_list = pd.concat(df, ignore_index=True)
                                eod_list['TRAN_MONTH'] = gl_table['gl']
                                eod_list = eod_list[~eod_list['CIF_ID'].isnull()]
                                eod_list = eod_list.replace(r'^\s*$',np.NaN,regex=True)
                                eod_list = eod_list.dropna()
                                eod_list.to_csv(self.data_dir + "eod_data/"+date1.date().isoformat()+".csv",index=False)
                                eod_data.append(eod_list)
                                # return eod_list
                                end = time.time()
                                print("raw data:", end-start)

                        eod_df = pd.concat(eod_data, 0)

                        eod_df = eod_df.drop_duplicates()
                        eod_df = eod_df[~eod_df['CIF_ID'].isnull()]
                        end = time.time()
                        print('eod_data - shape:', eod_df.shape,
                              "duration:", end - start)
                        return eod_df
                        
                except Exception as e:
                        print("eod_data:", e)

        def extract_dpd_before(self,enddate):
                try:
                        start = time.time()
                        loan_start_date = enddate
                        loan_start_date = loan_start_date.replace(day=1)
                        # print("loan_start_date:", loan_start_date)
                        loan_start_date_12m_before = loan_start_date + relativedelta(months=-12)
                        start_date=enddate.replace(day=1)
                        #end_period=enddate.strftime('%Y%m')
                        start_period=loan_start_date.strftime('%Y%m')
                        query = f"""
                                SELECT
                                            a.CIF_ID,max(A.DPD) as MAX_DPD_BEFORE
                                        FROM
                                            analytics_stg.uganda_loans a,
                                            ugedw.ug_gam@edw g
                                        WHERE
                                            g.bank_id = '56'
                                            AND g.foracid = a.ACCOUNT_NUM
                                            AND REPORT_MONTH< '{loan_start_date.strftime('%Y-%m-%d').upper()}' 
                                            AND REPORT_MONTH>= '{loan_start_date_12m_before.strftime('%Y-%m-%d').upper()}'
                                GROUP BY a.CIF_ID
                                """
                        df = []
                        # print(query)
                        for chunk in pd.read_sql(query, self.conn, chunksize=10000):
                                df.append(chunk)
                        maxdpdb4 = pd.concat(df, ignore_index=True)
                        maxdpdb4 = maxdpdb4.drop_duplicates()
                        
                        end = time.time()
                        print('maxdpdb4 - shape:', maxdpdb4.shape,
                              "duration:", end - start, "date:", loan_start_date.date().isoformat())
                        return maxdpdb4
                except Exception as e:
                        print("maxdpdb4:", e)
                        raise e

        def extract_dpd_after(self,enddate):
                try:
                        start = time.time()
                        loan_date = enddate
                        # print("loan_date:", loan_date)
                        loan_end_date_12m = (enddate) + pd.DateOffset(years=1)
                        # print("loan_end_date_12m:", loan_end_date_12m)
                        cols = ['CIF_ID', 'MAX_DPD_AFTER']
                        query = f"""
                                SELECT
                                            a.CIF_ID,max(a.DPD) MAX_DPD_AFTER
                                        FROM
                                            analytics_stg.uganda_loans a,
                                            ugedw.ug_gam@edw g
                                        WHERE
                                            g.bank_id = '56'
                                            -- AND g.CIF_ID IN ('50620412146') AND a.CIF_ID IN ('50620412146') 
                                            AND g.foracid = a.ACCOUNT_NUM
                                            -- AND g.SCHM_TYPE = 'LAA'
                                            AND REPORT_MONTH> '{loan_date.strftime('%Y-%m-%d').upper()}' 
                                            AND REPORT_MONTH <= '{loan_end_date_12m.strftime('%Y-%m-%d').upper()}'
                                GROUP BY a.CIF_ID
                                """
                        df = []
                        # print(query)
                        # exit()
                        for chunk in pd.read_sql(query, self.conn, chunksize=10000):
                                df.append(chunk)
                                
                        if len(df) != 0:
                                maxdpdaft = pd.concat(df, ignore_index=True)
                        else:
                                maxdpdaft = pd.DataFrame(columns=cols)
                        maxdpdaft = pd.concat(df, ignore_index=True)
                        
                        end = time.time()
                        print('maxdpdaft - shape:', maxdpdaft.shape,
                              "duration:", end - start)
                        return maxdpdaft
                except Exception as e:
                        print("maxdpdafter:", e)
                        raise e          
                        
        def fund_transfer(self,enddate):
                try:
                        # print("step 1")
                        start = time.time()
                        start_date = (enddate) + pd.offsets.MonthBegin(-2)
                        # print("start_date:", start_date)
                        daterange = pd.date_range(start=start_date, periods=1, freq='M')
                        # print(daterange)
                        ft_list = []
                        # print("step 2")
                        for dt in daterange:
                                print(dt)
                        #def run_months(dt):
                                # print("step 2.1")
                                startdate = (dt) + pd.offsets.MonthBegin(-1)
                                # print(startdate)
                                query = f"""
                                select id,
                                        ftfrom,
                                        ftto,
                                        ftamount,
                                        ftcif,
                                        ftcharge,
                                        ftcurrcode,
                                        fttrantime,
                                        ftphoneid,
                                        fttype,
                                        beneficiary_bank
                                from analytics_stg.stg_funds_transfer_trans_sub
                                WHERE 
                                created_at BETWEEN '{startdate.strftime('%d-%b-%y')}' and '{dt.strftime('%d-%b-%y')}' 
                                        -- and ftcif in ('50620450557','50620862059', '50620035369', '50620468520') 
                                        AND upper(substr(ftcif,1,2) ) = '56'
                                        and FTRESULT in ('Success','222','111','000')
                                        and FTAMOUNT IS NOT NULL
                                        and LENGTH(TRIM(TRANSLATE (FTAMOUNT, ' +-.0123456789',' '))) is null
                                        AND ftto != 'Empty'
                                        and UPPER(FTTYPE) NOT LIKE '%LOAN%'
                                and (fttype IS NULL
                                OR (fttype NOT LIKE '%ATM%'
                                AND upper(fttype) NOT LIKE '%AIRTIME%'
                                AND upper(fttype) NOT LIKE '%AGENT%'
                                AND upper(fttype) NOT LIKE '%FUNDSOWNACC%' ))
                                """
                                # funds = pd.read_sql(query,conn)
                                df = []
                                # print("step 3")
                                for chunk in pd.read_sql(query, self.conn, chunksize=10000):
                                        chunk['ENDING_PERIOD'] = pd.to_datetime(dt + pd.offsets.MonthEnd(0))
                                        df.append(chunk)

                                # print("step 4")
                                funds = pd.concat(df, ignore_index=True)
                                # print("step 5")
                                ft_list.append(funds)
                                # return funds
                        funds = pd.concat(ft_list)

                        # funds = pd.concat(ft_list, 0)
                        end = time.time()
                        print('fund_transfer - shape:', funds.shape,
                              "duration:", end - start)
                        return funds
                except Exception as e:
                        print("fund_transfer:", e)
        
        def currency_codes(self):
                try:
                        start = time.time()
                        query = f"""
                               SELECT
                                last_day(rtlist_date) rtlist_date,
                                round(AVG(var_crncy_units),2) var_crncy_units,
                                fxd_crncy_code
                                FROM
                                ugedw.curr_rate_sub
                                WHERE
                                bank_id = '56'
                                GROUP BY
                                bank_id,
                                ( last_day(rtlist_date) ),
                                last_day(rtlist_date),
                                fxd_crncy_code
                                """
                        # crncy = pd.read_sql(query,conn)
                        df = []
                        for chunk in pd.read_sql(query, self.conn_ebods, chunksize=10000):
                                df.append(chunk)
                        crncy = pd.concat(df, ignore_index=True)
                        # crncy.to_csv(self.data_dir + "others/currency_codes.csv",
                        #         index=False)
                        end = time.time()
                        print('currency_codes - shape:', crncy.shape,
                              "duration:", end - start)
                        return crncy
                except Exception as e:
                        print("currency_codes:", e)
        
        def gamdata(self):
                try:
                        start = time.time()
                        query = f"""
                               SELECT
                                distinct foracid,cif_id from ugedw.ug_gam
                                WHERE
                                bank_id = '56' and schm_type in ('CAA','SBA')
                                """
                        # gam = pd.read_sql(query,conn)
                        df = []
                        for chunk in pd.read_sql(query, self.conn_ebods, chunksize=100000):
                                df.append(chunk)
                        gam = pd.concat(df, ignore_index=True)
                        # gam.to_csv(self.data_dir + "others/gam_data.csv",
                        #         index=False)
                        end = time.time()
                        print('gam - shape:', gam.shape,
                              "duration:", end - start)
                        return gam
                except Exception as e:
                        print("gamdata:", e)

        def residence_out(self):
                try:
                        start = time.time()
                        query = f"""
                        SELECT distinct CIF_ID from ugedw.ug_gam
                                where BANK_ID='56' and CIF_ID is not null
                                AND (SCHM_CODE in ('SB115'))"""
                                
                        rd_out = pd.read_sql(query, self.conn_ebods)
                        end = time.time()
                        print('residence_out- shape:', rd_out.shape,
                              "duration:", end - start)
                        return rd_out
                except Exception as e:
                        print("residence_out:", e)

        def pensioners(self, enddate):
                try:

                        start = time.time()
                        gam = self.gamdata()
                        #pd.read_csv(self.data_dir + "others/gam_data.csv")
                        print('enddate:', enddate)
                        total_pensioners = []
                        months_i = list(range(1, 4))
                        print("also here")
                        for i in months_i:
                        #def run_months(i):
                                #date = datetime.now()
                                date1 = pd.to_datetime(enddate) + pd.offsets.MonthEnd(-i)
                                print('date1:', date1)
                                year = date1.strftime('%Y')
                                print(year)
                                month = date1.strftime('%m')
                                gl = (f"""{str('STATS_SUB_P')}{str(year)}{str(month)}""")
                                ym = (f"""{str(year)}{str(month)}""")
                                gl_table = {'gl': gl, 'year': year, 'month': month, 'ym': ym}
                                print(gl_table)
                                trans_query=f"""
                                SELECT
                                /*+ PARALLEL */
                                FORACID,'PENSIONER' CUST_TYPE
                                FROM ugedw.{gl_table['gl']} 
                                where BANK_ID='56' AND (upper(tran_particular) like '%PENSION%'  or upper(tran_rmks) like'%PENSION%')
                                and schm_type in ('CAA','SBA') AND PART_TRAN_TYPE='C'
                                
                                """
                                pen_list = pd.read_sql(trans_query, self.conn_ebods)
                                #pen_list.to_csv(self.data_dir + "pensioners/" + date1.date().isoformat() + ".csv",
                                        #index=False)
                                print(pen_list.head())
                                total_pensioners.append(pen_list)
                                # return pen_list
                                print('Almost done')

                        print("done")

                        # p = Pool(6)
                        # result = p.map_async(run_months, months_i)
                        # res = result.get()
                        # pensioners_df = pd.concat(res)
                        pensioners_df = pd.concat(total_pensioners, 0)
                        gam['FORACID'] = gam['FORACID'].astype(str)
                        pensioners_df = pd.merge(pensioners_df,gam, on='FORACID',how='left').copy()
                        pensioners_df = pensioners_df.drop_duplicates('CIF_ID')
                        end = time.time()
                        print('pensioners- shape:', pensioners_df.shape,
                              "duration:",end - start)
                        return pensioners_df
                except Exception as e:
                        print("pensioners:", e)
                        raise e

        
        def remmittance(self,enddate):
                try:
                        start = time.time()
                        total_sal = []

                        for i in range(1, 4):
                                date1 = pd.to_datetime(enddate + pd.offsets.MonthEnd(-i))
                                year = date1.strftime('%Y')
                                month = date1.strftime('%m')
                                gl = (f"""{str('STATS_SUB_P')}{str(year)}{str(month)}""")
                                ym = (f"""{str(year)}{str(month)}""")
                                gl_table = {'gl': gl, 'year': year, 'month': month, 'ym': ym}
                                # print(gl_table)
                                sal_query = f"""
                                select distinct g.cif_id,g.mode_of_oper_code, 'SALARY' cust_type
                                        from ugedw.{gl} a,ugedw.ug_gam g 
                                        --where a.foracid in ('4011100806471')
                                        where a.bank_id = '56' and
                                        g.bank_id='56' and a.acid=g.acid and a.part_tran_type='C'
                                        --and upper(tran_particular) not like '%PENSION%'
                                        AND (upper(tran_particular) not like '%EAZZYBIZ TRANSFER TO%')
                                        AND (upper(tran_particular) not like '%EAZZYBIZ TRF%')
                                        and (
                                                upper(tran_particular) like'%SALARY%' or upper(tran_rmks) like'%SALARY%' 
                                                or upper(tran_particular)like '% SALARY' or upper(tran_rmks) like 'SALARY / REMITTANCE' 
                                                or upper(tran_rmks) like 'SALARY_REMITTANCE' 
                                                or upper(tran_particular)like ' SAL%' or upper(tran_rmks) like ' SAL%' 
                                                or upper(tran_particular)like 'SAL %' or upper(tran_rmks) like 'SAL %'
                                                or upper(tran_particular) like '% SAL %'
                                                or upper(tran_particular) like '% SAL'
                                                or (upper(tran_rmks) like'%EAZZYBIZ%' and a.schm_code in ('SB100','SB120'))  
                                                or ((upper(tran_particular) like '%PENSION%'  or upper(tran_rmks) like'%PENSION%'))
                                        )
                                        and g.schm_type IN ('CAA','SBA')
                                        and g.mode_of_oper_code='SELF'
                                
                                """
                                # sal_list = pd.read_sql(sal_query, conn)
                                df = []
                                for chunk in pd.read_sql(sal_query, self.conn_ebods, chunksize=10000):
                                        df.append(chunk)
                                sal_list = pd.concat(df, ignore_index=True)
                                
                                total_sal.append(sal_list)
                                # return sal_list

                        total_sal1 = pd.concat(total_sal, 0)
                        # p = Pool(6)
                        # result = p.map_async(run_months, months_i)
                        # res = result.get()
                        # total_sal1 = pd.concat(res)
                        total_sal1 = total_sal1.drop_duplicates('CIF_ID')
                        end = time.time()
                        print("remmittance - shape:",total_sal1.shape, 'duration:',end-start)
                        return total_sal1
                
                except Exception as e:
                        print("remmittance:", e)

        def other_policy(self):
                try:
                        start = time.time()
                        # Residence outside Kenya
                        query = """
                        SELECT ORGKEY,
                        --1    Max Applicant Age > 75 (Business)  Finacle
                        (CASE  WHEN ((TRUNC(SYSDATE)-TRUNC(CUST_DOB))/365)> 75 THEN 1 ELSE 0 END) AS AGE75,
                        --2    Main Age > 60 (Salaried)   Finacle
                        (CASE  WHEN ((TRUNC(SYSDATE)-TRUNC(CUST_DOB))/365)> 60 THEN 1 ELSE 0 END) AS AGE60,
                        --3    Customer Age <18   Finacle
                        (CASE  WHEN ((TRUNC(SYSDATE)-TRUNC(CUST_DOB))/365)<18 THEN 1 ELSE 0 END) AS AGE18,
                        --4    Main Occupation Student    Finacle
                        ( CASE WHEN OCCUPATION='STUD' THEN 1 ELSE 0 END ) AS  STUDENT,
                        --9    Equity staff   Finacle
                        ( CASE WHEN STAFFEMPLOYEEID is  not null  or STAFFFLAG='Y' THEN 1 ELSE 0 END) AS STAFF
                        from
                        ugedw.stg_accounts
                        WHERE  BANK_ID='56'
                        """
                        # generator_df = pd.read_sql(query, conn)
                        df = []
                        for chunk in pd.read_sql(query, self.conn_ebods, chunksize=10000):
                                df.append(chunk)
                        other_policies = pd.concat(df, ignore_index=True)
                        end = time.time()
                        print("other policies data - shape:", other_policies.shape, 'duration:', end - start, "seconds")
                        return other_policies
                except Exception as e:
                        print("other_policy:", e)

        def staff(self):
                try:
                        start = time.time()
                        query = f"""
                               select cif_id,mode_of_oper_code , 'STAFF' Cust_Type
                                from Ugedw.ug_gam
                                where Bank_Id='56' and mode_of_oper_code is not null and schm_type='SBA'
                                and schm_code in ('SB190')
                                """
                        staff = pd.read_sql(query,self.conn_ebods)
                        end = time.time()
                        print("staff data - shape:", staff.shape, 'duration:', end - start, "seconds")
                        return staff
                except Exception as e:
                        print("staff data:", e)

        def get_crb_userids(self):
                try:
                        start = time.time()
                        query = f"""
                               WITH Q1 AS (
                                    SELECT acc.orgkey AS cif_id, name,
                                    (CASE WHEN nat_id_card_num IS NOT NULL THEN nat_id_card_num
                                        WHEN passportno IS NOT null THEN passportno
                                        WHEN IDTYPEC1 IS NOT null THEN IDTYPEC1 
                                        END) AS unique_id,
                                     (CASE WHEN nat_id_card_num IS NOT NULL THEN 'NationalID'
                                        WHEN passportno IS NOT null THEN 'PassportNo' 
                                        WHEN IDTYPEC1 IS NOT null THEN 'BusinessRegno' 
                                        END) AS id_type
                                    FROM ugedw.stg_accounts acc
                                    LEFT JOIN ugedw.stg_corporate cpr 
                                    ON acc.core_cust_id = cpr.core_cust_id
                                    WHERE acc.bank_id='56')
                                SELECT * FROM Q1
                                """
                        crb_user_ids = pd.read_sql(query,self.conn_ebods)
                        crb_user_ids.to_csv(self.data_dir + "others/crb_user_ids.csv",
                                index=False)
                        end = time.time()
                        print("get_crb_userids data - shape:", crb_user_ids.shape, 'duration:', end - start, "seconds")
                        return crb_user_ids
                except Exception as e:
                        print("crb_user_ids data:", e)

        def fin_obligations(self):
                try:
                        start = time.time()
                        query_fin = f"""
                            SELECT 
                                gam.cif_id,
                                MAX(FLOW_AMT) FIN_MONTHLY_OBLIGATIONS ---GET THE MAX INSTALMENT IN EXISTING LOANS
                            FROM stg_lrs L,stg_LAM a,ug_gam gam
                            WHERE L.bank_id = '56'
                            and a.bank_id = '56'
                            and L.acid=a.acid
                            and DMD_SATISFY_MTHD not in ('D','N')
                            and gam.bank_id = '56'
                            AND gam.acct_cls_date IS NULL
                            and L.acid = gam.acid
                            GROUP BY
                            gam.cif_id
                        """
                        # query_way4 = """
                        #     select a.account,
                        #     a.TOTAL_DUE_ON_CREDIT_CARDS,
                        #     (a.CREDIT_CARD_LIMIT * 0.2) as WAY4_OBLIGATIONS
                        #     from
                        #     (select account, abs(sum(TOTAL_DUE)) as TOTAL_DUE_ON_CREDIT_CARDS,
                        #     abs(sum(credit_limit)) as CREDIT_CARD_LIMIT 
                        #     from analytics_stg.stg_olb_credit_cards
                        #     where SNP_DATE=trunc(sysdate-1)
                        #     group by account)a
                        # """    
                        query_gam = """
                            select gam.cif_id, 
                            gam.foracid,
                            TRUNC(SYSDATE) AS OBLIGATION_DATE from ug_gam gam
                            where gam.bank_id = '56'
                            AND gam.acct_cls_date IS NULL
                         """             
                        # obligations = pd.read_sql(query,conn)
                        df_fin = pd.read_sql(query_fin,self.conn_ebods)
                        #df_way4 = pd.read_sql(query_way4,self.conn)
                        df_gam = pd.read_sql(query_gam,self.conn_ebods)

                        # df_way4.rename(columns = {'ACCOUNT':'FORACID'},inplace = True)
                        # df_way4['FORACID'] = df_way4['FORACID'].astype(str)
                        df_gam['FORACID'] = df_gam['FORACID'].astype(str)
                        df_gam['CIF_ID'] = df_gam['CIF_ID'].astype(str)
                        df_fin['CIF_ID'] = df_fin['CIF_ID'].astype(str)
                        # df_way4_final = pd.merge(df_way4, df_gam, how='inner')
                        # df_way4_final.fillna(0, inplace=True)
                        df_fin.fillna(0, inplace = True)
                        #data = pd.merge(df_fin, df_way4_final, how = 'outer')
                        data = df_fin
                        data.fillna(0, inplace = True)
                        data['TOTAL_OBLIGATION'] = data['FIN_MONTHLY_OBLIGATIONS'] 
                        #+ data['WAY4_OBLIGATIONS']
                        
                        end = time.time()
                        print("obligations data - shape:",data.shape, 'duration:',end-start,"seconds")
                        return data
                except Exception as e:
                        print("obligations data:", e)

if __name__ == '__main__':
        enddate = pd.to_datetime('2021-10-31')
        c = Metadata()
        c.transaction_data(enddate)
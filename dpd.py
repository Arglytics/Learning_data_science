from django.forms import SplitDateTimeField
import numpy as np
import pandas as pd
import glob
import os
import dateutil.parser as dparser

gf = "E:/repos/JULIET.ONDISI/UG_Loan_Book/2020/uglb_202007.xlsx"
xls_file = pd.ExcelFile(gf)
sheet_names = [sheet for sheet in xls_file.sheet_names if keyword.upper() in sheet.upper()]
print("sheet name",sheet_names)
gf1 = pd.read_excel(gf,sheet_name='UG_Book_as_at_31.07.2020 (2)')
gf2 = pd.read_excel(gf,sheet_name='UG_Book_as_at_31.07.2020')
df = pd.concat([df.assign(sheet_name=name) for name, df in gf1.items()])
df.columns = df.columns.str.replace(' ','')
cols = df.columns
for i in cols:
    df[i] = df[i].replace('.','')
    df[i]=df[i].apply(lambda x: x.strip())

path1 = "E:\\repos\\JULIET.ONDISI\\UG_Loan_Book\\2019\\"

read_files = []
allfiles = []
err_files = []
data = []
h = 0

keyword = 'LOAN'
excel_files=glob.glob(os.path.join(str(path1), "*.xlsx"))
print(excel_files)
for x in excel_files:
    #b= dparser.parse(x,fuzzy=True)
    #print(b)
    REPORT_MONTH = x.rsplit("\\")[-1]
    print("report month",REPORT_MONTH)
    print(x)
    allfiles.append(x)
    try:
        xls_file = pd.ExcelFile(x)
        #sheet_names = [sheet for sheet in xls_file.sheet_names]
        sheet_names = [sheet for sheet in xls_file.sheet_names if keyword.upper() in sheet.upper()]
        print("sheet name",sheet_names)
        dfs = pd.read_excel(x,sheet_name=sheet_names)
        df = pd.concat([df.assign(sheet_name=name) for name, df in dfs.items()])
        #print(df.head(2))
        df['REPORT_MONTH'] = REPORT_MONTH
        df.columns = df.columns.str.upper()
        df.columns = df.columns.str.replace(' ','')
        df.rename(columns={'ACCTCRNCY':'CURR'},inplace=True)
        df1 = df[['CIFID','SCHMCODE','ACCTNUM','ACCTNAME','CURR','DPD','DISAMT','BALANCE_UGX','REPORT_MONTH']]
        cols = df1.columns
        
        read_files.append(x)
    except Exception as e:
        print("file has errors:",e)
        err_files.append(x)
        continue
    #dt = pd.concat(df, axis=0, ignore_index=True)
    data.append(df1)
    h = h + 1

data1 = pd.concat(data,0)

print("read_files",len(read_files))
data1.info()
data1[data1['ACCTNAME'].isnull()]
data2 = data1[~data1['CIFID'].isnull()]
data2.info()
#data2[data2['ACCTNAME'].isnull()]
cols1 = ['CIF_ID','SCHM_CODE','ACCT_NUMBER','ACCT_NAME','ACCT_CURRENCY','DPD','DISB_AMT','BALANCE_UGX','REPORT_MONTH']
data2.columns = cols1
data2['CIF_ID'] = data2['CIF_ID'].astype(str).replace('\.0', '', regex=True)
data2['ACCT_NUMBER'] = data2['ACCT_NUMBER'].astype(str).replace('\.0', '', regex=True)
data2['BALANCE_UGX'] = abs(data2['BALANCE_UGX'])
data2['BALANCE_UGX'] = data2['BALANCE_UGX'].astype(float)
data2['DISB_AMT'] = data2['DISB_AMT'].astype(float)
data2['DPD'] = data2['DPD'].astype(int)
data2.head()
data2['REPORT_MONTH'] = data2['REPORT_MONTH'].str[5:11]

data2['REPORT_MONTH'].unique()
data2.head()

data2.to_csv("E:/repos/JULIET.ONDISI/UG_Loan_Book/2019.csv",index=False)

dw = pd.read_csv('E:/repos/JULIET.ONDISI/UG_Loan_Book/2019.csv',low_memory=False)
dw1 = pd.read_csv('E:/repos/JULIET.ONDISI/UG_Loan_Book/2020.csv',low_memory=False)
dw2 = pd.read_csv('E:/repos/JULIET.ONDISI/UG_Loan_Book/2021.csv',low_memory=False)
dw3 = pd.read_csv('E:/repos/JULIET.ONDISI/UG_Loan_Book/2022.csv',low_memory=False)

combined = pd.concat([dw,dw1,dw2,dw3],axis = 0)
combined= combined.reset_index(drop=True)
combined['DPD'] = combined['DPD'].astype(int)
combined['CIF_ID'] = combined['CIF_ID'].astype(str)
combined['SCHM_CODE'] = combined['SCHM_CODE'].astype(str)
combined.tail(10)
combined['ACCT_CURRENCY'].unique()
combined['SCHM_CODE'].unique()
combined.rename(columns={'BALANCE_UGX':'OUTSTANDING_BALANCE_UGX'},inplace=True)

combined[combined['CIF_ID']=='56010944135']

combined.to_csv("E:/repos/JULIET.ONDISI/UG_Loan_Book/UG_COMBINED_LB.csv",index=False)

################OVERDRAFTS

path2 = "E:/repos/JULIET.ONDISI/UG_Loan_Book/ovedrafts.xlsx"
xls_file = pd.ExcelFile(path2)
sheetname = xls_file.sheet_names
data = []
for sn in sheetname:
    print(sn)
    df = pd.read_excel(path2,sheet_name=sn)
    df['REPORT_MONTH'] = sn
    #print(df.columns)
    df.rename(columns={'PRODUCT':'SCHM_CODE','ACCT_NUM':'ACCT_NUMBER','CURR':'ACCT_CURRENCY','SANCT_LIM':'DISB_AMT',
                            'OSTBAL':'OUTSTANDING_BALANCE_UGX'},inplace=True)						
    cols1 = ['CIF_ID','SCHM_CODE','ACCT_NUMBER','ACCT_NAME','ACCT_CURRENCY','DPD','DISB_AMT','OUTSTANDING_BALANCE_UGX','REPORT_MONTH']
    df1 = df[cols1]
    df1.shape
    data.append(df1)

data1 = pd.concat(data,0)
data1.head()
data1.info()

data2 = data1.copy()
data2 = data2[~data2['CIF_ID'].isnull()]
data2['CIF_ID'] = data2['CIF_ID'].astype(str).replace('\.0', '', regex=True)
data2['ACCT_NUMBER'] = data2['ACCT_NUMBER'].astype(str).replace('\.0', '', regex=True)
data2['OUTSTANDING_BALANCE_UGX'] = abs(data2['OUTSTANDING_BALANCE_UGX'])
data2['OUTSTANDING_BALANCE_UGX'] = data2['OUTSTANDING_BALANCE_UGX'].astype(float)
data2['DISB_AMT'] = data2['DISB_AMT'].astype(float)
data2['DPD'] = data2['DPD'].astype(int)

combined = pd.read_csv("E:/repos/JULIET.ONDISI/UG_Loan_Book/UG_COMBINED_LB.csv",low_memory=False)

combineds1 = pd.concat([data2,combined],axis = 0)

combineds1.to_csv("E:/repos/JULIET.ONDISI/UG_Loan_Book/UG_COMBINED_LB_v3.csv",index=False)

sg = pd.read_csv("E:/scoring/uganda/ug_loan_book/ug_loanOd_201901_202204.csv",low_memory=False)
combined3 = pd.read_csv("E:/repos/JULIET.ONDISI/UG_Loan_Book/UG_COMBINED_LB_v2.csv",low_memory=False)
sg.info()
sg['CIF ID'].count()
sg['DPD'].unique() 
sg[sg['Acct Crncy'].isnull()]

combined3.info()
combined3['CIF_ID'].unique() 


g1 = pd.read_csv("E:/scoring/uganda/ug_loan_book/"+"ug_loanOd_201901_202204.csv",low_memory=False)
g1 = g1.sort_values(by=['CIF_ID','REPORT_MONTH'],ascending=True)
g1['CIF_ID'] = g1['CIF_ID'].astype(str)
g1.shape
g2 = pd.read_csv("E:/repos/JULIET.ONDISI/UG_Loan_Book/UG_COMBINED_LB_v2.csv",low_memory=False)
g2 = g2.sort_values(by=['CIF_ID','REPORT_MONTH'],ascending=True)
g2.shape

g1.tail()
g2.tail()
g2['REPORT_MONTH'].unique()

g1[g1['CIF_ID']=='1001200921528']
g2[g2['CIF_ID']=='56100216845']

gam = f"""select cif_id,schm_code
                                from Ugedw.ug_gam@EDW
                                where Bank_Id='56'
                                and schm_type in ('CAA','SBA')"""

dg = pd.read_sql(gam,conn)
dg.rename(columns={'CIF_ID':'CIF_ID_GAM'},inplace=True)
dg['CIF_ID_GAM1'] = dg['CIF_ID_GAM'].copy()

data['ACCT_NUMBER'] = data['ACCT_NUMBER'].astype(str)

data = pd.merge(g1,dg['CIF_ID_GAM'],right_on='CIF_ID_GAM',left_on='CIF_ID',how='left')
data1 = pd.merge(data,dg['CIF_ID_GAM1'],right_on='CIF_ID_GAM1',left_on='ACCT_NUMBER',how='left')

data1.head()

data1['CIF_ORIG'] = np.where(data1['CIF_ID_GAM'].isnull(),data1['CIF_ID_GAM1'],data1['CIF_ID_GAM'])

data1.head()
data2 = data1[['CIF_ORIG','SCHM_CODE', 'ACCT_NUMBER', 'ACCT_NAME', 'ACCT_CURRENCY',
       'DPD', 'DISB_AMT', 'OUTSTANDING_BALANCE_UGX', 'REPORT_MONTH']]
data3 = data2.drop_duplicates()
data3.info()

g1[g1.duplicated(['ACCT_NUMBER','REPORT_MONTH'])]
g1[g1['CIF_ID']=='1001500870017']





import pandas as pd
from string import Template
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from Utils.config_wrapper.wrapper import BaseHookWrapper, VariableWrapper
from Datasets.pipelines.Spark.base.utils import SMTPConnect
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os import listdir
from Projects.Agency.Dormancy.POS_Agents_weekly import *

agency_week_results = pos_agency_weekly_results()
agency_week_results3 = agency_week_results.copy()

agency_week_results3['RM'] = agency_week_results3['RM'].fillna('HEADOFFICE')
agency_week_results3 = agency_week_results3.replace({np.nan: None})
agency_week_results3 = agency_week_results3.replace({0: None})
agency_week_results3['BRANCH'] = agency_week_results3['BRANCH'].str.replace(
    '/', '_')
agency_week_results3 = agency_week_results3.loc[agency_week_results3['BRANCH'] != 'daadab']

Branches = []


def Branch_email():
    for Branch in agency_week_results3['BRANCH'].unique():
        Branch_data = agency_week_results3[agency_week_results3['BRANCH'] == Branch].copy(
        )
        brnch = Branch_data['BRANCH'].unique()
        Branches.append(brnch)
        print(Branch_data['BRANCH'].unique())
        Branch_data = Branch_data.reset_index(drop=True)
        Branch_data1 = Branch_data.copy()
        Branch_data1['STAFF_EMAIL'] = Branch_data1.STAFF_EMAIL.str.split(
            ';').str[0]
        Branch_colstodrop = Branch_data1.columns[0:1].tolist(
        ) + Branch_data1.columns[8:14].tolist() + Branch_data1.columns[19:].tolist()
        Branch_data1.drop(Branch_colstodrop, inplace=True, axis=1)
        Branch_colseq = Branch_data1.columns[0:1].tolist(
        ) + Branch_data1.columns[11:].tolist() + Branch_data1.columns[1:11].tolist()
        Branch_data1 = Branch_data1[Branch_colseq]

        if Branch_data['STAFF_EMAIL'].isnull().all():
            supervisor_email = Branch_data['BGDM_EMAIL'].unique().tolist()
            cc_email = Branch_data['OPS_EMAIL'].unique().tolist()
            names = Branch_data.BGDM_EMAIL.str.split(
                '.').str[0].unique().tolist()
        else:
            supervisor_email = Branch_data['STAFF_EMAIL'].unique().tolist()
            cc_email = Branch_data['BGDM_EMAIL'].unique(
            ).tolist()+Branch_data['OPS_EMAIL'].unique().tolist()
            names = Branch_data.STAFF_EMAIL.str.split(
                '.').str[0].unique().tolist()

        cc_email = list(filter(None, cc_email))

        if supervisor_email == [None]:
            supervisor_email = []
            cc_email = []
            names = []
        else:
            supervisor_email = supervisor_email
            cc_email = cc_email
            names = names

        Branch_data1.to_csv('Projects/Agency/Dormancy/'+Branch+'.csv')

        message = MIMEMultipart()
        smtp = BaseHookWrapper.get_connection("smtp")
        smtp_con = SMTPConnect(smtp.host, smtp.port)

        sender_email = 'juliet.ondisi@equitybank.co.ke'
        toemails = supervisor_email
        ccemails = cc_email
        names = names

        if toemails == ccemails:
            toemails = toemails
            ccemails = []

        else:
            toemails = toemails
            ccemails = ccemails

        filename = Branch+'.csv'
        path = os.getcwd()+'/Projects/Agency/Dormancy/'+filename
        filefullpath = [os.getcwd()+'/Projects/Agency/Dormancy/'+filename]

        if names == []:
            print('no names to send to')
        else:
            subject = 'Weekly_Branch_report'

            message_template = Template('{:<10}'.format(
                """
            Hello ${PERSON_NAME},

            Please find attached the weekly Branch report on dormant POS agents.

            Regards,
            Agency Team
            """))

            smtp_con.send_file("no_reply@equitybank.co.ke", names, toemails, ccemails, subject, message_template,
                               filefullpath, [filename])
        os.remove(path)


regions = []


def reg_manager_email():
    for rm in agency_week_results3['RM'].unique():
        rm_data = agency_week_results3[agency_week_results3['RM'] == rm].copy()
        reg = agency_week_results3['RM'].unique()
        print(reg)
        regions.append(reg)
        rm_data = rm_data.reset_index(drop=True)
        rm_data1 = rm_data.copy()
        rm_colstodrop = rm_data1.columns[8:14].tolist() + rm_data1.columns[0:1].tolist(
        ) + rm_data1.columns[20:21].tolist() + rm_data1.columns[24:].tolist()
        rm_data1.drop(rm_colstodrop, inplace=True, axis=1)

        rm_colseq = rm_data1.columns[0:1].tolist() + rm_data1.columns[11:12].tolist()\
            + rm_data1.columns[1:11].tolist() + rm_data1.columns[12:].tolist()
        rm_data1 = rm_data1[rm_colseq]

        summary1 = rm_data.groupby('BRANCH').agg({'TRANSACTING_ACC': 'nunique',
                                                  'OUTLET_CODE': 'nunique',
                                                  'TERMINAL_ID': 'nunique'
                                                  }).reset_index()
        summary1.columns = ['Branch', 'No of Agents',
                            'No of Outlets', 'No of Terminals']

        for col in summary1.columns[1:]:
            summary1[col] = summary1[col].astype(int)

        Branch_summary1 = summary1

        writer = pd.ExcelWriter(
            '/home/user/project/Projects/Agency/Dormancy/Regional_report_'+rm+'.xlsx', engine='xlsxwriter')

        Branch_summary1.to_excel(writer, sheet_name='summary', index=False)
        rm_data1.to_excel(writer, sheet_name='trans_info', index=False)
        writer.save()
        writer.close()

        message = MIMEMultipart()
        smtp = BaseHookWrapper.get_connection("smtp")
        smtp_con = SMTPConnect(smtp.host, smtp.port)
        if rm_data['RM_EMAIL'].isnull().all():
            rm_email = []
            names = []
        else:
            rm_email = rm_data['RM_EMAIL'].unique().tolist()
            names = rm_data.RM_EMAIL.str.split('.').str[0].unique().tolist()
        sender_email = 'juliet.ondisi@equitybank.co.ke'
        toemails = rm_email
        ccemails = []
        names = names
        subject = 'Regional Weekly Report_'+rm

        filename = 'Regional_report_'+rm+'.xlsx'

        path = os.getcwd()+'/Projects/Agency/Dormancy/'+filename
        filefullpath = [os.getcwd()+'/Projects/Agency/Dormancy/'+filename]

        if names == []:
            print('no names to send to')
        else:
            subject = 'Regional_Weekly_report'

            message_template = Template('{:<10}'.format(
                """
            Hello ${PERSON_NAME},

            Please find attached the regional weekly report on POS agents dormancy.

            Regards,
            Agency Team
            """))

            smtp_con.send_file("no_reply@equitybank.co.ke", names, toemails, ccemails, subject, message_template,
                               filefullpath, [filename])
        os.remove(path)


def head_office_email():
    summary1 = agency_week_results3.groupby('RM').agg({'BRANCH': 'nunique', 'TRANSACTING_ACC': 'nunique',
                                                       'OUTLET_CODE': 'nunique', 'TERMINAL_ID': 'nunique'
                                                       }).reset_index()
    summary1.columns = ['RM', 'No Of Branches',
                        'No of agents', 'No of outlets', 'No of Terminals']

    for col in summary1.columns[1:]:
        summary1[col] = summary1[col].astype(int)
    regional_summary1 = summary1

    # Branch
    summary1 = agency_week_results3.groupby('BRANCH').agg({'TRANSACTING_ACC': 'nunique',
                                                           'OUTLET_CODE': 'nunique', 'TERMINAL_ID': 'nunique'
                                                           }).reset_index()
    summary1.columns = ['Branch', 'No of agents',
                        'No of outlets', 'No of Terminals']

    for col in summary1.columns[1:]:
        summary1[col] = summary1[col].astype(int)

    Branch_summary1 = summary1

    # trans)info
    ho_colseq = agency_week_results3.columns[1:2].tolist() + agency_week_results3.columns[18:19].tolist() + agency_week_results3.columns[2:8].tolist()\
        + agency_week_results3.columns[14:18].tolist() + agency_week_results3.columns[19:20].tolist()\
        + agency_week_results3.columns[24:25].tolist()
    trans_info = agency_week_results3[ho_colseq]

    writer = pd.ExcelWriter(
        '/home/user/project/Projects/Agency/Dormancy/head_office.xlsx', engine='xlsxwriter')

    regional_summary1.to_excel(writer, sheet_name='rm_summary', index=False)
    Branch_summary1.to_excel(writer, sheet_name='Branch_summary', index=False)
    trans_info.to_excel(writer, sheet_name='trans_info', index=False)

    writer.save()
    writer.close()

    message = MIMEMultipart()
    smtp = BaseHookWrapper.get_connection("smtp")
    smtp_con = SMTPConnect(smtp.host, smtp.port)
    sender_email = 'juliet.ondisi@equitybank.co.ke'
    toemails = ['gabriel.odhiambo@equitybank.co.ke']
    print(toemails)
    ccemails = ['rebecca.kariuki@equitybank.co.ke', 'Dennis.Njau@equitybank.co.ke',
                'michael.maina@equitybank.co.ke', 'juliet.ondisi@equitybank.co.ke', 'Robert.Maina@equitybank.co.ke']
    names = ['Gabriel']
    print(ccemails)
    subject = 'Summarized POS Agents Dormancy weekly Report'

    filename = 'head_office.xlsx'

    path = os.getcwd()+'/Projects/Agency/Dormancy/'+filename
    filefullpath = [os.getcwd()+'/Projects/Agency/Dormancy/'+filename]

    message_template = Template('{:<10}'.format(
        """
    Hello ${PERSON_NAME},

    Please find attached the summarized weekly report on POS agents dormancy for all regions and branches.

    Regards,
    Agency Team
    """))

    smtp_con.send_file("no_reply@equitybank.co.ke", names, toemails, ccemails, subject, message_template,
                       filefullpath, [filename])
    os.remove(path)


if __name__ == "__main__":
    Branch_email()
    reg_manager_email()
    head_office_email()

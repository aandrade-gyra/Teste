from dash.dependencies import Input, Output
try:
    from .mongoChartsFunctions import *
except:
    from mongoChartsFunctions import *
from dash import callback_context
from dash import dash_table
import plotly.express as px
from dash import html, dcc
import pandas as pd
import numpy as np
import requests
import datetime
import pymongo
import base64
import sys
import os
import re

def scr(client,value):
    list_components = []
    aux = list(client['gyramais']['Business'].find({'number':int(value)},{'cnpj':1}))[0]
    cnpj = aux['cnpj']
    businessID = aux['_id']
    users = pd.json_normalize(list(client['gyramais']['_User'].find(
        {'_p_currentBusiness' : 'Business$'+businessID},
        {'fullName' : 1, 'email' : 1, 'phoneNumber' : 1, 'cpf' : 1}))).fillna('N/A')
    # SCR INTEGRATIONS
    if client['gyramais']['IntegrationSCR'].count_documents({'businessNumber' : int(value)}) > 0:
        
        scrVariablesTranslate = {
            'installOverdrawn' : 'Parcela Crédito Especial',
            'installRotative' : 'Parcela Crédito Rotativo',
            'installCreditCard' : 'Parcela Cartão de Crédito',
            'installPayrollLoan' : 'Parcela Crédito Consignado',
            'installRealState' : 'Parcela Crédito Imobiliário',
            'installLoan' : 'Parcela Outros Créditos',
            'loanDue' : 'Outros Créditos a Vencer',
            'payrollLoanDue' : 'Crédito Consignado a Vencer',
            'realStateDue' : 'Crédito Imobiliário a Vencer',
            'creditCardDue' : 'Cartão Crédito a Vencer',
            'loanOverdue' : 'Outros Créditos Vencidos',
            'payrollLoanOverdue' : 'Crédito Consignado Vencido',
            'realStateOverdue' : 'Crédito Imobiliário Vencido',
            'creditCardOverdue' : 'Cartão de Crédito Vencido'
        }

        SCRtranslate = {
            'date' : 'Data',
            'cnpj' : 'CNPJ',
            'cpf' : 'CPF',
            'summary.amount' : 'Risco Crédito',
            'summary.totalResponsabilityAmount' : 'Risco Total de Crédito',
            'summary.vendorIndirectRiskAmount' : 'Risco Indireto de Crédito',
            'summary.coObligationsAmount' : 'Coobrigações',
            'summary.institutionQuantity' : 'Qtd. Instituições',
            'summary.transactionsQuantity' : 'Qtd. transações',
            'summary.toExpireAmount' : 'A Vencer',
            'summary.toExpireAmount30Days' : 'A Vencer 30 dias',
            'summary.expiredAmount' : 'Total Atraso',
            'summary.expiredAmount30Days' : 'Atraso B',
            'summary.expiredAmount31to60Days' : 'Atraso C',
            'summary.expiredAmount61to90Days' : 'Atraso D',
            'summary.expiredAmount91to180Days' : 'Atraso E F G',
            'summary.expiredAmount181to360Days' : 'Atraso H',
            'summary.expiredAmountMoreThan360Days' : 'Atraso > 1 ano',
            'summary.lossAmount' : 'Perdas Totais',
            'summary.lossAmountTo12Months' : 'Perdas até 12M',
            'summary.lossAmountMoreThan12Months' : 'Perdas > 12M',
            'summary.creditLimitAmount' : 'Limite de Crédito',
            'summary.creditLimitAmount360Days' : 'Limite Crédito (360 dias)',
            'summary.creditLimitAmountMoreThan360Days' : 'Limite Crédito (+ que 360 dias)'
        }


        featureEngtranslateAmountCnpj = {
             
            'toExpireAmount30DaysDivRevenueAmount' : 'Valor a expirar em 30 dias / Faturamento auto-declarado',
            'toExpireAmount30DaysDivOrderSum3MCnpj' : 'Valor a expirar em 30 dias / Soma das vendas nos últimos 3 meses',
            'toExpireAmount30DaysDivOrderSum6MCnpj' : 'Valor a expirar em 30 dias / Soma das vendas nos últimos 6 meses',
            'toExpireAmount30DaysDivOrderSum12MCnpj' : 'Valor a expirar em 30 dias / Soma das vendas nos últimos 12 meses',
            'amountDivRevenueAmountCnpj' : 'Faturamento / Faturamento auto-declarado',
            'amountDivOrderSum3MCnpj' : 'Faturamento / Faturamento auto-declarado 3 meses',
            'amountDivOrderSum6MCnpj' : 'Faturamento / Faturamento auto-declarado 6 meses',
            'amountDivOrderSum12MCnpj' : 'Faturamento / Faturamento auto-declarado 12 meses',
            'totalRiskDivRevenueAmountCnpj' : 'Risco Total / Faturamento auto-declarado',
            'cpDivRevenueAmountCnpj' : 'cp / Faturamento auto-declarado',
            'mpDivRevenueAmountCnpj' : 'mp / Faturamento auto-declarado',
            'lpDivRevenueAmountCnpj' : 'lp / Faturamento auto-declarado'
            
        }

        featureEngtranslateAmountCpf = {

            'mpDivRevenueAmountCpf' : 'mp / Faturamento auto-declarado',
            'lpDivRevenueAmountCpf' : 'lp / Faturamento auto-declarado',
            'toExpireAmount30DaysDivRevenueAmountCpf' : 'Valor a expirar em 30 dias / Faturamento auto-declarado',
            'toExpireAmount30DaysDivOrderSum3MCpf' : 'Valor a expirar em 30 dias / Faturamento auto-declarado 3 meses',
            'toExpireAmount30DaysDivOrderSum6MCpf' : 'Valor a expirar em 30 dias / Faturamento auto-declarado 6 meses',
            'amountDivOrderSum3MCpf' : 'Valor / Faturamento auto-declarado 3 meses',
            'amountDivRevenueAmountCpf' : 'Valor / Faturamento auto-declarado ',
            'totalRiskDivRevenueAmountCpf' : 'Risco total / Faturamento auto-declarado'


        }    

        dfSCR = pd.json_normalize(list(client['gyramais']['IntegrationSCR'].find({'businessNumber' : int(value)},{'cnpj' : 1,'cpf' : 1,'date' : 1,'transactions' : 1, 'businessNumber' : 1, 'summary' : 1})))
        
        try:
            scrVariables = pd.json_normalize(list(client['ds-gyramais']['scrVariables'].aggregate([{'$match':{'_id.businessNumber':int(value)}},{'$sort':{'_id.anomes' : -1}}]))[0])
            featureEng = pd.json_normalize(list(client['ds-gyramais']['featureEngVariables'].aggregate([{'$match':{'_id.businessNumber':int(value)}},{'$sort':{'_id.anomes' : -1}}]))[0])
            

        except:
            scrVariables = []
            featureEng = []

        
        if len(dfSCR) > 0:
            if 'cpf' not in dfSCR.columns:
                dfSCR['cpf'] = np.nan

            dfSCRSummary = dfSCR[list(SCRtranslate.keys())].rename(columns=SCRtranslate)

            dfSCRTransaction = pd.DataFrame([])
            for i in dfSCR[['date','cnpj','cpf','transactions']].values:
                if len(i[3]) > 0:
                    dfSCRTransaction = pd.concat([dfSCRTransaction, pd.concat([pd.DataFrame([i[:3]],columns=['Data','CNPJ','CPF']),pd.DataFrame(i[3])],axis=1).fillna(method='ffill')  ])

            if len(dfSCR) > 0:

                list_components.append(html.H5('SCR', style={}))

            flagScr = False
            flagFeatEng = False    

            if len(dfSCRSummary) > 0 and len(dfSCRSummary[(dfSCRSummary['CPF'].isnull())&(dfSCRSummary['CNPJ'] == cnpj)]) > 0:
                    
                list_components.append(html.H6('Visão CNPJ', style={}))
                # teste
                if len(scrVariables) > 0:
                    flagScr = True
                    aux = { key+'_cnpj' : scrVariablesTranslate[key] for key in scrVariablesTranslate.keys()}
                    for feat in list(aux.keys()):
                        if feat not in scrVariables.columns:
                            scrVariables.loc[:,feat] = 'N/A'
                        else:
                            scrVariables.loc[:,feat] = scrVariables.loc[:,feat].round(2)
                
                if len(featureEng) > 0:
                    flagFeatEng = True
                    aux2 = { key: featureEngtranslateAmountCnpj[key] for key in featureEngtranslateAmountCnpj.keys()}
                    for feat in list(aux2.keys()):
                        if feat not in featureEng.columns:
                            featureEng.loc[:,feat] = 'N/A'
                        else:
                            featureEng.loc[:,feat] = featureEng.loc[:,feat].round(2)    
                    
                    list_components.append(
                        html.Div([
                                html.Div(children=create_dash_table(scrVariables[list(aux.keys())].rename(columns=aux).transpose().reset_index().rename(columns={0 : 'R$', 'index' : ''}),14,'auto')),
                                html.Div(children=create_dash_table(featureEng[list(aux2.keys())].rename(columns=aux2).transpose().reset_index().rename(columns={0 : '%', 'index' : ''}),14,'auto')),
                        ],style={'width': '100%', 'display': 'inline-block'})
                    )  

                elif flagScr and flagFeatEng == False:
                    list_components.append(
                            html.Div([
                                    html.Div(children=[create_dash_table(scrVariables[list(aux.keys())].rename(columns=aux).transpose().reset_index().rename(columns={0 : 'R$', 'index' : ''}),14,'auto')],style={'width':'50%','display': 'inline-block'}),
                            ],style={'width': '100%', 'display': 'inline-block'})
                    )
                elif flagScr == False and flagFeatEng == True:
                    list_components.append(
                            html.Div([
                                    html.Div(children=[create_dash_table(featureEng[list(aux2.keys())].rename(columns=aux2).transpose().reset_index().rename(columns={0 : 'creditLimitAmountRiskCnpj', 1:'creditLimitAmountUsageCnpj', 'index' : ''}),14,'auto')],style={'width':'50%','display': 'inline-block'}),
                            ],style={'width': '100%', 'display': 'inline-block'})
                    )
                else:
                    pass                           
                #
                if flagScr and flagFeatEng:
                    groupvars = ['creditLimitAmountRiskCnpj','creditLimitAmountUsageCnpj','creditLimitAmountVariationCnpj']
                    if any([i in groupvars for i in featureEng.columns]):
                        list_components.append(
                            createGroupingInformation(featureEng,groupvars,'SCR CNPJ')
                        )
                   
                #
                df = dfSCRSummary[(dfSCRSummary['CPF'].isnull())&(dfSCRSummary['CNPJ'] == cnpj)].sort_values('Data').copy()
                df.loc[:,'Data'] = df['Data'].dt.strftime('%Y-%m')
                if len(df[df['Risco Total de Crédito'] > 0]) > 0:
                    list_components.append(create_scr_graph(df[['Risco Total de Crédito','Data','CNPJ']].drop_duplicates().groupby(['Data','CNPJ'],as_index=False).sum().tail(24),'Risco Crédito (R$)','Risco Total de Crédito','Data','Risco de Crédito - CNPJ','CNPJ'))
                list_components.append(
                    html.Details([
                        html.Summary('Histórico'),
                        html.Div(children=[create_dash_table(df.drop_duplicates().drop(columns=['CNPJ','CPF']),int(np.clip(len(df),1,15)),'auto')])
                    ],open=True)  
                )

            if len(dfSCRTransaction) > 0 and len(dfSCRTransaction[(dfSCRTransaction['CPF'].isnull())&(dfSCRTransaction['CNPJ'] == cnpj)]) > 0:

                df = dfSCRTransaction[(dfSCRTransaction['CPF'].isnull())&(dfSCRTransaction['CNPJ'] == cnpj)].drop(columns=['CPF']).drop_duplicates().sort_values('Data').copy()
                df.loc[:,'Data'] = df['Data'].dt.strftime('%Y-%m')
                df = pd.pivot_table(df,index=['CNPJ','domain','subdomain','code','type'],columns=['Data'],aggfunc='sum').reset_index(drop=False)
                df.columns = [''.join(i) if '' == i[1] else i[1] for i in df.columns]

                list_components.append(
                    html.Details([
                        html.Summary('Categorias'),
                        html.Div(children=[create_dash_table(df,int(np.clip(len(df),1,15)),'auto')])
                    ],open=True)
                )
            # Visão CNPJ Relacionados
            if len(dfSCRSummary) > 0 and len(dfSCRSummary[(dfSCRSummary['CPF'].isnull())&(dfSCRSummary['CNPJ'] != cnpj)]) > 0:

                list_components.append(html.H6('Visão CNPJ de Relacionados', style={}))
                # Por enquanto não temos 
                #if len(scrVariables) > 0:
                #    aux = { key+'_relatedCnpj' : scrVariablesTranslate[key] for key in scrVariablesTranslate.keys()}
                #    for feat in list(aux.keys()):
                #        if feat not in scrVariables.columns:
                #            scrVariables.loc[:,feat] = 'N/A'
                #        else:
                #            scrVariables.loc[:,feat] = scrVariables.loc[:,feat].round(2)
                #    list_components.append(create_dash_table(scrVariables[list(aux.keys())].rename(columns=aux).transpose().reset_index().rename(columns={0 : 'R$', 'index' : ''}),14,'auto'))
                #
                df = dfSCRSummary[(dfSCRSummary['CPF'].isnull())&(dfSCRSummary['CNPJ'] != cnpj)].sort_values('Data').copy()
                df.loc[:,'Data'] = df['Data'].dt.strftime('%Y-%m')
                if len(df[df['Risco Total de Crédito'] > 0]) > 0:
                    list_components.append(create_scr_graph(df[['Risco Total de Crédito','Data','CNPJ']].drop_duplicates().groupby(['Data','CNPJ'],as_index=False).sum().tail(24),'Risco Crédito (R$)','Risco Total de Crédito','Data','Risco de Crédito - CNPJ','CNPJ'))
                list_components.append(
                    html.Details([
                        html.Summary('Histórico'),
                        html.Div(children=[create_dash_table(df.drop_duplicates().drop(columns=['CPF']),int(np.clip(len(df),1,15)),'auto')])
                    ],open=True)  
                )

            if len(dfSCRTransaction) > 0 and len(dfSCRTransaction[(dfSCRTransaction['CPF'].isnull())&(dfSCRTransaction['CNPJ'] != cnpj)]) > 0:

                df = dfSCRTransaction[(dfSCRTransaction['CPF'].isnull())&(dfSCRTransaction['CNPJ'] != cnpj)].drop(columns=['CPF']).drop_duplicates().sort_values('Data').copy()
                df.loc[:,'Data'] = df['Data'].dt.strftime('%Y-%m')
                df = pd.pivot_table(df,index=['CNPJ','domain','subdomain','code','type'],columns=['Data'],aggfunc='sum').reset_index(drop=False)
                df.columns = [''.join(i) if '' == i[1] else i[1] for i in df.columns]

                list_components.append(
                    html.Details([
                        html.Summary('Categorias'),
                        html.Div(children=[create_dash_table(df,int(np.clip(len(df),1,15)),'auto')])
                    ],open=True)
                )
            # Visão CPF
            if len(dfSCRSummary) > 0 and len(dfSCRSummary[~dfSCRSummary['CPF'].isnull()]) > 0:

                list_components.append(html.H6('Visão CPF', style={}))
                if len(users) > 0:
                    list_components.append(
                        html.Details(
                            [
                                html.Summary('Informação de Avalistas'),
                                html.Div(children=[
                                    create_dash_table(users.drop(columns=['_id']),height='auto')

                                ]),
                            ]
                        )
                    )
                if len(scrVariables) > 0:
                    aux = { key+'_cpf' : scrVariablesTranslate[key] for key in scrVariablesTranslate.keys()}
                    for feat in list(aux.keys()):
                        if feat not in scrVariables.columns:
                            scrVariables.loc[:,feat] = 'N/A'
                        else:
                            scrVariables.loc[:,feat] = scrVariables.loc[:,feat].round(2)

                if len(featureEng) > 0:
                    flagFeatEng = True
                    aux3 = { key: featureEngtranslateAmountCpf[key] for key in featureEngtranslateAmountCpf.keys()}
                    for feat in list(aux3.keys()):
                        if feat not in featureEng.columns:
                            featureEng.loc[:,feat] = 'N/A'
                        else:
                            featureEng.loc[:,feat] = featureEng.loc[:,feat].round(2)
                    
                    list_components.append(
                        html.Div([
                                html.Div(children=create_dash_table(scrVariables[list(aux.keys())].rename(columns=aux).transpose().reset_index().rename(columns={0 : 'R$', 'index' : ''}),14,'auto')),
                                html.Div(children=create_dash_table(featureEng[list(aux3.keys())].rename(columns=aux3).transpose().reset_index().rename(columns={0 : '%', 'index' : ''}),14,'auto')),
                        ],style={'width': '100%', 'display': 'inline-block'})
                    )  
                            
                    
                #
                if flagScr and flagFeatEng:
                    groupvars = ['creditLimitAmountRiskCpf','creditLimitAmountUsageCpf','creditLimitAmountVariationCpf']
                    if any([i in groupvars for i in featureEng.columns]):
                        list_components.append(
                            createGroupingInformation(featureEng,groupvars,'SCR CPF')
                        )
                    
                #
                df = dfSCRSummary[~dfSCRSummary['CPF'].isnull()].sort_values('Data').copy()
                df.loc[:,'Data'] = df['Data'].dt.strftime('%Y-%m')
                if len(df[df['Risco Total de Crédito'] > 0]) > 0:
                    list_components.append(create_scr_graph(df[['Risco Total de Crédito','Data','CPF']].drop_duplicates().groupby(['Data','CPF'],as_index=False).sum().tail(24),'Risco Crédito (R$)','Risco Total de Crédito','Data','Risco de Crédito - CPF','CPF'))
                list_components.append(
                    html.Details([
                        html.Summary('Histórico'),
                        html.Div(children=[create_dash_table(df.drop_duplicates().drop(columns=['CNPJ']),int(np.clip(len(df),1,15)),'auto')])
                    ],open=True)  
                )

            if len(dfSCRTransaction) > 0 and len(dfSCRTransaction[~dfSCRTransaction['CPF'].isnull()]) > 0:
                
                df = dfSCRTransaction[~dfSCRTransaction['CPF'].isnull()].drop_duplicates().sort_values('Data').copy()
                df.loc[:,'Data'] = df['Data'].dt.strftime('%Y-%m')
                df = pd.pivot_table(df,index=['CPF','domain','subdomain','code','type'],columns=['Data'],aggfunc='sum').reset_index(drop=False)
                df.columns = [''.join(i) if '' == i[1] else i[1] for i in df.columns]

                list_components.append(
                    html.Details([
                        html.Summary('Categorias'),
                        html.Div(children=[create_dash_table(df,int(np.clip(len(df),1,15)),'auto')])
                    ],open=True)
                )
    return list_components
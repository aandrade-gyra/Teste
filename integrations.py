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

def integrations(client,value):
    list_components = []
    integrations = client['gyramais']['Integration'].find({'businessNumber' : int(value)},{'typeName' : True})
    integrations = list(set([ i['typeName'] for i in list(integrations)]))

    # instability found on theses queries. When I query with any date, it may not come back with everything.
    lastMonthWanted = return_backward_months(int(datetime.datetime.now().strftime('%Y%m')),15)[-1]
    dfAggOrder = pd.json_normalize(client['gyramais']['GroupedOrder'].find({'_id.businessNumber' : value }))
    dfAggPayment = pd.json_normalize(client['gyramais']['GroupedPayment'].find({'_id.businessNumber' : value }))
    dfAggBalancedAmount = pd.json_normalize(client['gyramais']['GroupedBalancedAmount'].find({'_id.businessNumber' : value,  'balancedAmount' : { '$ne' : None} }))
    
    try:
        dfAggOrder = dfAggOrder.loc[dfAggOrder['anomes'] >= lastMonthWanted]
        dfAggOrder['_id.date'] = dfAggOrder['_id.date'].dt.strftime('%Y-%m')
    except:
        pass
    try:
        dfAggPayment = dfAggPayment.loc[dfAggPayment['anomes'] >= lastMonthWanted]
        dfAggPayment['_id.date'] = dfAggPayment['_id.date'].dt.strftime('%Y-%m')
    except:
        pass
    try:
        dfAggBalancedAmount = dfAggBalancedAmount.loc[dfAggBalancedAmount['anomes'] >= lastMonthWanted]
        dfAggBalancedAmount['_id.date'] = dfAggBalancedAmount['_id.date'].dt.strftime('%Y-%m')
    except:
        pass
    
    try:
    
        if len(dfAggOrder) > 0 and len(dfAggPayment) > 0:

            list_integrations = list(set(list(dfAggOrder['_id.integrationTypeName'].unique()) + list(dfAggPayment['_id.integrationTypeName'].unique()) + integrations))

        elif len(dfAggOrder) > 0 and len(dfAggPayment) == 0:

            integrations = list(set(list(dfAggOrder['_id.integrationTypeName'].unique()) + integrations))

        elif len(dfAggOrder) == 0 and len(dfAggPayment) >= 0:

            integrations = list(set(list(dfAggPayment['_id.integrationTypeName'].unique()) + integrations))
    
    except:
        pass



    for i in list(set(integrations)):
        
        if i not in ['OrderIn', 'OrderOut', 'PaymentIn', 'PaymentOut', 'ProScore', 'ProScore CPF','SCR','SCR CPF', 'Data Trust']:
            aux = False
            #Show Name
            if len(dfAggOrder) > 0:
                if len(dfAggOrder[dfAggOrder['_id.integrationTypeName'] == i]) > 0:
                    aux = True

            if len(dfAggPayment) > 0:
                if len(dfAggPayment[dfAggPayment['_id.integrationTypeName'] == i]) > 0:
                    aux = True

            if aux == True:
                
                list_components.append(html.H5(i, style={}))
                
                # Account information
                contact = pd.json_normalize( list( client['gyramais']['Integration'].aggregate([
                    {'$match' : {'businessNumber' : int(value), 'typeName' : i}},
                    {'$unwind' : { 'path' : '$contact.bank', 'preserveNullAndEmptyArrays' : True}},
                    {'$project' : {'contact' : 1}}
                ])))
                # special case ML
                ml = []
                if i in ['Mercado Livre','Mercado Pago']:

                    ml = list(client['gyramais']['Integration'].find({'businessNumber' : value, 'typeName' : i}, {'credentials' : 1}).limit(1))

                if len(contact) > 0 and len(ml) > 0:
                    contact = contact.drop(columns=['_id']).T.reset_index().rename(columns={'index' : 'Info'})
                    list_components.append(
                        html.Details(
                            [
                                html.Summary('Informação de conta'),
                                html.Div(children=[create_dash_table(
                                    contact,
                                    int(np.clip(len(contact),1,15)),
                                    'auto'),
                                html.A('Link para usuário ML', href='https://api.mercadolibre.com/users/'+str(ml[0]['credentials']['user_id']),target="_blank", style={})
                                ]),
                            ]
                        )
                    )
                elif len(contact) > 0:
                    contact = contact.drop(columns=['_id']).T.reset_index().rename(columns={'index' : 'Info'})
                    list_components.append(
                        html.Details(
                            [
                                html.Summary('Informação de conta'),
                                html.Div(children=[create_dash_table(
                                    contact,
                                    int(np.clip(len(contact),1,15)),
                                    'auto')
                                ]),
                            ]
                        )
                    )
            # End of Account information beggining to show graphs
            if len(dfAggOrder) > 0:
                if len(dfAggOrder[(dfAggOrder['_id.integrationTypeName'] == i)]) > 0:
                    #Order
                    list_components.append(create_grouped_graph(dfAggOrder[(dfAggOrder['_id.integrationTypeName'] == i)],'R$','sum','Soma - Vendas - ' + i,'stack'))
                    list_components.append(create_grouped_graph(dfAggOrder[(dfAggOrder['_id.integrationTypeName'] == i)],'QTD','count','Quantidade - Vendas - '+i,'stack'))

                    if 'acquirers' in dfAggOrder[(dfAggOrder['_id.integrationTypeName'] == i)].columns:
                        df = []
                        for dic in dfAggOrder.loc[(dfAggOrder['_id.integrationTypeName'] == i),['acquirers','anomes']].to_dict(orient='records'):
                            if type(dic['acquirers']) == list and len(dic['acquirers']) > 0:
                                df.append({'anomes':dic['anomes'],'adquirentes': ', '.join(dic['acquirers'][:-1])})
                        if len(df) > 0:
                            df = pd.DataFrame(df).sort_values('anomes',ascending=False)
                            if len(df) > 0:
                                list_components.append(
                                    html.Details(
                                        [
                                            html.Summary('Adquirentes encontrados'),
                                            html.Div(children=[create_dash_table(
                                                df,
                                                10,
                                                'auto')
                                            ]),
                                        ]
                                    )
                                )

            if len(dfAggPayment) > 0:
                if len(dfAggPayment[(dfAggPayment['_id.integrationTypeName'] == i)]) > 0:
                    #Payment
                    list_components.append(create_grouped_graph(dfAggPayment[(dfAggPayment['_id.integrationTypeName'] == i)],'R$','sum','Cashflow - Pagamentos - '+i,'group'))
                    list_components.append(create_grouped_graph(dfAggPayment[(dfAggPayment['_id.integrationTypeName'] == i)],'QTD','count','Cashflow - Quantidade - '+i,'stack'))
                    #Adding detailed information
                    df = get_description_payments(client, i, value)
                    if len(df) > 0:
                        list_components.append(
                            html.Details(
                                [
                                    html.Summary('Informação detalhada da conta'),
                                    html.Div(children=[create_dash_table(
                                        df,
                                        10,
                                        'auto')
                                    ]),
                                ]
                            )
                        )

                    if 'relationships' in dfAggPayment[(dfAggPayment['_id.integrationTypeName'] == i)].columns:
                        df = []
                        for dic in dfAggPayment.loc[(dfAggPayment['_id.integrationTypeName'] == i),['relationships','anomes']].to_dict(orient='records'):
                            if type(dic['relationships']) == list and len(dic['relationships']) > 0:
                                for i in dic['relationships']:
                                    df.append({'anomes':dic['anomes'],'relacionamento':i,'Contagem':1})
                        if len(df) > 0:
                            df = pd.DataFrame(df)
                            df = df.groupby(['anomes','relacionamento'],as_index=False).count().sort_values('anomes',ascending=False)
                            if len(df) > 0:
                                list_components.append(
                                    html.Details(
                                        [
                                            html.Summary('Informação de Relacionamentos'),
                                            html.Div(children=[create_dash_table(
                                                df,
                                                10,
                                                'auto')
                                            ]),
                                        ]
                                    )
                                )

                    if len(dfAggBalancedAmount) > 0 and len(dfAggBalancedAmount[(dfAggBalancedAmount['_id.integrationTypeName'] == i)]) > 0:
                        list_components.append(create_grouped_graph(dfAggBalancedAmount[(dfAggBalancedAmount['_id.integrationTypeName'] == i)].fillna(0),'R$','balancedAmount','Saldo Em Conta - Pagamentos - '+i,'group'))

                    if i in ['Mercado Livre', 'Mercado Pago'] and len(dfAggPayment[(dfAggPayment['_id.integrationTypeName'] == i) & (dfAggPayment['_id.description'].isin(['third_party_credit', 'withdraw','money_transfer','merchant_credit']))]) > 0:
                        # Information about other Loans Etc.
                        list_components.append(html.H6('Outros Possíveis créditos', style={}))
                        dfAux = pd.pivot_table(dfAggPayment[(dfAggPayment['_id.integrationTypeName'] == i) & (dfAggPayment['_id.description'].isin(['third_party_credit', 'withdraw','money_transfer','merchant_credit']))],index=dfAggPayment[(dfAggPayment['_id.integrationTypeName'] == i) & (dfAggPayment['_id.description'].isin(['third_party_credit', 'withdraw','money_transfer','merchant_credit']))].anomes, columns='_id.description',values='sum', aggfunc='sum').dropna(how='all').fillna(0)
                        dfAux = pd.concat([dfAux,pd.DataFrame([dfAux.sum().values],columns=dfAux.columns,index=['Soma'])],axis=0)
                        list_components.append(create_dash_table(dfAux.reset_index().rename(columns={'index' : ''}),int(np.clip(len(dfAux),1,15)),'auto'))

    if client['gyramais']['OCRStatement'].count_documents({ 'businessNumber' : int(value) }) > 0:

        list_components.append(html.H5('Extratos Bancários', style={}))

        list_components.append(
            html.Details(
                [
                    html.Summary('Informação'),
                    html.Div(children=[create_dash_table(
                        pd.json_normalize(list(client['gyramais']['OCRStatement'].find({ 'businessNumber' : int(value) },{ 'pointsFraud' : 1, '_p_file' : 1, 'bankname' : 1, 'filename' : 1}))).drop(columns='_id'),
                        height='auto'
                        )
                    ]),
                ]
            )
        )

        df = pd.json_normalize(list(client['gyramais']['OCRStatement'].aggregate([
            {
                '$match' : 
                { 
                    'businessNumber' : int(value), 'Error' : { '$exists' : False } 
                }
            },
            {
                '$project' : {
                    'balance' : 1
                }
            },
            {
                '$unwind' : {
                    'path' : '$balance'
                }
            },
            {
                '$addFields' : {
                    'date' : { '$toDate' : { '$concat' : [ {'$substr': [ '$balance.day.date', 0, 4 ]}, '-', {'$substr': [ '$balance.day.date', 5, 2 ]} , '-01'] } }
                }
            },{
                '$sort' : {
                    'date' : 1
                }
            },{
                '$group' : {
                    '_id' : { 'date' : '$date'},
                    'sum' : { '$last' : '$balance.day.amount'} 
                }
            }
        ])))
        if len(df) > 0:
            list_components.append(create_grouped_graph(df,'R$','sum','Saldo Mensal','group'))
        
        df = pd.json_normalize(list(client['gyramais']['OCRStatement'].aggregate([
            {
                '$match' : 
                { 
                    'businessNumber' : int(value), 'Error' : { '$exists' : False } 
                }
            },
            {
                '$project' : {
                    'bank_statement' : 1
                }
            },
            {
                '$unwind' : {
                    'path' : '$bank_statement'
                }
            },
            {
                '$project' : {
                    'Data' :  '$bank_statement.transaction.date',
                    'Valor (R$)' : '$bank_statement.transaction.amount',
                    'Tipo' : '$bank_statement.transaction.type',
                    'Descrição' : '$bank_statement.transaction.source',
                }
            }
        ])))
        if len(df) > 0:
            list_components.append(create_grouped_graph(df.rename(columns={'Data' : '_id.date', 'Tipo' : '_id.type', 'Valor (R$)' : 'sum'}),'R$','sum','Saldo Mensal','group',tickFormat='%-d-%m-%Y'))
            list_components.append(
                html.Details(
                    [
                        html.Summary('Informação detalhada do Extrato'),
                        html.Div(children=[create_dash_table(
                            df.drop(columns='_id'),
                            height='auto'
                            )
                        ]),
                    ]
                )
            )
    #
    try:
            
            featureEng = pd.json_normalize(list(client['ds-gyramais']['featureEngVariables'].aggregate([{'$match':{'_id.businessNumber':int(value)}},{'$sort':{'_id.anomes' : -1}}]))[0])
        
    except:
            
            featureEng = []


        
    if len(dfAggOrder) == 0:

        dfAggOrder['_id.integrationTypeName'] = np.nan

    if len(dfAggPayment) == 0:

        dfAggPayment['_id.integrationTypeName'] = np.nan

    if len(dfAggOrder[(dfAggOrder['_id.integrationTypeName'].isin(['OrderOut','OrderIn']) )]) > 0 or len(dfAggPayment[(dfAggPayment['_id.integrationTypeName'].isin(['PaymentIn','PaymentOut']) )]) > 0:

        list_components.append(html.H5('Agrupamentos Integrações', style={}))

    
    if len(dfAggOrder[(dfAggOrder['_id.integrationTypeName'].isin(['OrderIn','OrderOut']))]) > 0:
        groupvars = ['ordersRisk','ordersUsage','ordersVariation']
        if any([i in groupvars for i in featureEng.columns]):
            list_components.append(
                createGroupingInformation(featureEng,groupvars,'Vendas')
            )    
        list_components.append(create_grouped_graph(dfAggOrder[(dfAggOrder['_id.integrationTypeName'].isin(['OrderIn','OrderOut']))],'R$','sum','Soma - Vendas','stack','multi'))

    
    if len(dfAggPayment[(dfAggPayment['_id.integrationTypeName'].isin(['PaymentIn','PaymentOut']))]) > 0 :
        groupvars = ['paymentsRisk','paymentsInUsage','paymentsInVariation']
        if any([i in groupvars for i in featureEng.columns]):
            list_components.append(
                createGroupingInformation(featureEng,groupvars,'Cashflow')
            )
        df = dfAggPayment[(dfAggPayment['_id.integrationTypeName'].isin(['PaymentIn']))].copy()
        df.loc[df['_id.integrationTypeName'] == 'PaymentIn','_id.integrationTypeName'] = 'netValue'
        for i,row in df[df['_id.integrationTypeName'] == 'netValue'].iterrows():
            df.loc[(df['_id.integrationTypeName'] == 'netValue')&(df['anomes']==row['anomes']),'sum'] = row['sum'] - dfAggPayment.loc[(dfAggPayment['_id.integrationTypeName'] == 'PaymentOut')&(dfAggPayment['anomes']==row['anomes']),'sum'].abs().mean()
        dfAggPayment = pd.concat([dfAggPayment,df],axis=0).reset_index(drop=True)
        list_components.append(create_grouped_graph(dfAggPayment[(dfAggPayment['_id.integrationTypeName'].isin(['PaymentIn','PaymentOut','netValue']))],'R$','sum','Cashflow - Pagamentos','group','multi'))
    
    #        


    return list_components
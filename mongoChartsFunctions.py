from configparser import ConfigParser
from dash import html
from dash import dcc
from dash import dash_table
import plotly.graph_objs as go
import plotly.express as px
from string import Template
from pathlib import Path
import pandas as pd
import urllib.parse
import numpy as np
import requests
import pymongo
import logging
import hashlib
import base64
import hmac
import time
import json

colors = [
    "#5284ff", "#295982", "#6fabd0", "#92c0d7", "#C9D3CC", "#d7f0e9", "#b0d5c7", "#477977", "#0BB599"
]

def get_login(user,pwd):
    site = 'https://graphql.gyramais.com.br/'
    query = """
        mutation {
            logIn (
                email: \"""" + user + """\"
                password : \"""" + pwd + """\"
                core : true
            )
            {
                id
                sessionToken
            }
        }
    """
    r = requests.post(site,timeout=6000,json={'query': query})
    try:
        d = json.loads(r.text)
        if len(d['data']['logIn']['id']) == 10 and d['data']['logIn']['sessionToken'].startswith('r:'):
            return True
        else:
            return False
    except:
        return False

def chech_if_core_loggedin(sessionToken):

    data = { "sessionToken" : sessionToken }
    base_url = 'https://api.gyramais.com.br/functions/session-token-to-user'
    header = { 'X-Parse-Application-Id' : 'cbc6e712-dc06-4c37-8720-ee2cef5b9a9d'}
    post = requests.post(base_url,timeout=6000,json=data, headers=header)
    if 'result' in post.json().keys() and 'username' in post.json()['result'].keys():
        if '@gyramais.com' in post.json()['result']['username']:
            return True
    else:
        return False

def setup_logger( name, log_file, level=logging.DEBUG): 

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')

    my_file = Path(log_file)
    # print("check the if condition for the file")
    # print(my_file.is_file())

    if my_file.is_file():
        #print(logging.getLogger(name).hasHandlers())
        # if logging.getLogger(name).hasHandlers():
        if len(logging.getLogger(name).handlers)>0:
            return logging.getLogger(name)
        else:
            handler = logging.FileHandler(log_file, mode='a')        
            handler.setFormatter(formatter)
            logger = logging.getLogger(name)
            logger.setLevel(level)
            logger.addHandler(handler)
            logger.propagate = False
            return logger
    else:
        handler = logging.FileHandler(log_file, mode='a')        
        handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.propagate = False
        return logger

def config(filename='./database.ini', section='mongodb'):
    parser = ConfigParser()
    parser.read(filename)
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

def return_backward_months(anomes,qtd,actualmonth=True):
    if actualmonth == True:
        meses = [anomes]
    else:
        meses = []
    mes = anomes
    for i in range(0,qtd):
        if (mes // 10**0 % 100) == 1:
            mes = mes + 11
            mes = mes - 100
        else:
            mes = mes - 1
        meses.append(int(mes))
    return meses

def create_dash_table(df,page_size=15,height='53vh'):
    df = df.round(2)
    table = dash_table.DataTable(
        id='table'+str(np.random.randint(0,1000)),
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
        style_cell={'textAlign': 'center', 'padding': '5px', 'font_size': '10px','font-family': 'Axiforma', 'height' :'auto'},
        style_as_list_view=True,
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }
        ],
        style_cell_conditional=[
            {
                'if': {'column_id': c},
                'textAlign': 'left'
            } for c in ['']
        ],
        page_current=0,
        page_size=page_size,
        page_action="native",
        sort_action='native',
        sort_mode='multi',
        style_header={
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold',
            'font_size': '9px',
            'maxWidth' : '50px',
            'textOverflow': 'ellipsis',
            'whiteSpace': 'normal'
        }
    )
    return html.Div(table,style={'overflowX': 'auto','overflowY': 'auto', 'height' : height, "margin": "1% 1%"})

def create_dash_table_with_tooltip(df,page_size=15,height='53vh'):
    df = df.round(2)
    table = dash_table.DataTable(
        id='table'+str(np.random.randint(0,1000)),
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
        tooltip_data=df.to_dict('records'),
        tooltip_duration=None,
        style_cell={'textAlign': 'center', 'padding': '5px', 'font_size': '10px','font-family': 'Axiforma', 'height' :'auto', 'overflow': 'hidden', 'textOverflow': 'ellipsis', 'maxWidth': 0},
        style_as_list_view=True,
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }
        ],
        style_cell_conditional=[
            {
                'if': {'column_id': c},
                'textAlign': 'left'
            } for c in ['']
        ],
        page_current=0,
        page_size=page_size,
        page_action="native",
        sort_action='native',
        sort_mode='multi',
        style_header={
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold',
            'font_size': '9px',
        },
        css=[{
            'selector': '.dash-table-tooltip',
            'rule': 'background-color: grey; font-family: monospace; color: white'
        }]
    )
    return html.Div(table,style={'height' : height, "margin": "1% 1%"})

def get_description_payments(client, integrationTypeName, businessNumber):

    try:
        df = pd.json_normalize(list(client['gyramais']['IntegrationPayment'].aggregate([
            {
                '$match' : {
                    'integrationTypeName':integrationTypeName,
                    'businessNumber' : businessNumber
                }
            },{
                '$sort' : {
                    'date' : -1
                }
            },{
                '$limit' : 300
            },{
                '$project' : {
                    'businessNumber' : 1,
                    'type' : 1,
                    'date' : 1,
                    'amount' : 1,
                    'description' : 1,
                    'category' : 1,
                    'balancedAmount' : 1
                }
            }
        ]))).drop(columns=['_id']).rename(columns={'date' : 'Data', 'amount' : 'Valor (R$)', 'balancedAmount' : 'Saldo (R$)', 'description' : 'Descrição', 'type' : 'tipo', 'category' : 'Categoria'})

        return df
    except:
        return pd.DataFrame([])

def getSectorPlatformSegmentVariables():
    
    client = pymongo.MongoClient(base64.b64decode(config()['url']).decode(), ssl=True)
    
    platformType = {
        i['field'] : i['result'] \
        for i in list(client['gyramais']['BusinessPlatformType'].aggregate([
            {
                '$project' :
                {
                  'result' : '$name',
                  'field' : {'$concat' : ['BusinessPlatformType',{ '$literal': '$' }, '$_id']}
                }
            }
        ]))
    }
    
    platformType['N/A'] = 'N/A'
    
    sector = {
        i['field'] : i['result'] \
        for i in list(client['gyramais']['BusinessSector'].aggregate([
            {
                '$project' :
                {
                  'result' : '$name',
                  'field' : {'$concat' : ['BusinessSector',{ '$literal': '$' }, '$_id']}
                }
            }
        ]))
    }
    
    sector['N/A'] = 'N/A'

    segment = {
        i['field'] : i['result'] \
        for i in list(client['gyramais']['BusinessSegment'].aggregate([
            {
                '$project' :
                {
                  'result' : '$name',
                  'field' : {'$concat' : ['BusinessSegment',{ '$literal': '$' }, '$_id']}
                }
            }
        ]))
    }
    
    segment['N/A'] = 'N/A'
    
    return platformType, sector, segment

def create_grouped_graph(df,yaxis,col,title,barmode,integration='normal',tickFormat="%m-%Y"):
    
    if '_id.status' not in df.columns:
        df.loc[:,'_id.status'] = ''
    if '_id.type' not in df.columns:
        df.loc[:,'_id.type'] = ''
        
    df.loc[:,'status type'] = df['_id.type'].fillna('') + ' ' +  df['_id.status'].fillna('')
    
    fig = go.Figure()

    if integration=='normal':
        
        for cond in df['status type'].unique():
            copy = df[df['status type'] == cond].groupby(['status type','_id.date'],as_index=False).sum().sort_values('_id.date').copy()
            fig.add_trace(go.Bar(
                        x=copy['_id.date'],
                        y=copy[col],
                        name=cond
                ))
            
    elif integration=='multi':
        
        for cond in df['_id.integrationTypeName'].unique():
            copy = df[df['_id.integrationTypeName'] == cond].groupby(['_id.integrationTypeName','_id.date'],as_index=False).sum().sort_values('_id.date').copy()
            fig.add_trace(go.Bar(
                        x=copy['_id.date'],
                        y=copy[col],
                        name=cond
                ))
        
    
    fig.update_layout(
        template = 'plotly_white',
        font=dict(
            family="Axiforma"
        ),
        xaxis_tickangle=-45,
        title={
            'text': title,
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Data",
        yaxis_title=yaxis,
        xaxis_tickformat=tickFormat,
        barmode=barmode,
        colorway=colors
    )
    fig.update_xaxes(nticks=len(df['_id.date'].unique()))
        
    return dcc.Graph(figure=fig,style={'background': "#FFFFFF", 'border' : 'none' , 'border-radius' : '2px', 'box-shadow' : '0 2px 10px 0 rgba(70, 76, 79, .2)'})

def create_scr_graph(df,yaxis_title,x_col,y_col,title,col='CPF'):

    fig = go.Figure()

    for doc in df[col].unique():
        fig.add_trace(go.Bar(
                x=df[y_col][df[col] == doc].copy(),
                y=df[x_col][df[col] == doc].copy(),
                name=doc
        ))
        
    fig.update_layout(
        template = 'plotly_white',
        font=dict(
            family="Axiforma"
        ),
        xaxis_tickangle=-45,
        title={
            'text': title,
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Data",
        yaxis_title=yaxis_title,
        xaxis_tickformat="%m-%Y",
        barmode='stack',
        colorway=colors #px.colors.sequential.Vivid
    )
    fig.update_xaxes(nticks=len(df[y_col][df[col] == doc].copy().unique()))
        
    return dcc.Graph(figure=fig,style={'background': "#FFFFFF", 'border' : 'none' , 'border-radius' : '2px', 'box-shadow' : '0 2px 10px 0 rgba(70, 76, 79, .2)'})

def createGroupingInformation(df,array,sufix=''):
    arrayFigsOrders = []
    if any([i in df.columns for i in array ]):
        for variaveis in array:
            if  'Variation' in variaveis:
                prefix = 'Negativa - ' if df[variaveis].max() == 0 else 'Estável - ' if df[variaveis].max() == 1 else 'Positiva - ' if df[variaveis].max() == 2 else 'N/A'     
            else:
                prefix = 'Baixo - ' if df[variaveis].max() == 0 else 'Médio - ' if df[variaveis].max() == 1 else 'Alto - ' if df[variaveis].max() == 2 else 'N/A'                        
            fig = go.Figure()
            fig.add_trace(
                go.Indicator(
                mode="number",
                value= df[variaveis].max(),
                number = {
                        'prefix':prefix,
                        'font':{'size':40}
                        },
                gauge = {'axis': {'visible': False}},
                domain = {'row': 0, 'column': 0}, 
                title = {'text': 'Risco '+sufix if 'Risk' in variaveis else 'Uso '+sufix if 'Usage' in variaveis else 'Variação '+sufix,'font':{'size':24}}
                )
            )
        
            fig.update_layout(
                    height=200
            )
            arrayFigsOrders.append(fig)
        
        return html.Div([
                html.Div([
                        dcc.Graph(id=array[0]+'gauge1',figure=arrayFigsOrders[0],style={'background': "#FFFFFF", 'border' : 'none' , 'border-radius' : '1px', 'box-shadow' : '0 1px 1px 0 rgba(70, 76, 79, .2)'})
                    ],className='tripleFigure',style={  }),
                    html.Div([
                        dcc.Graph(id=array[1]+'gauge2',figure=arrayFigsOrders[1],style={'background': "#FFFFFF", 'border' : 'none' , 'border-radius' : '1px', 'box-shadow' : '0 1px 1px 0 rgba(70, 76, 79, .2)'})
                    ],className='tripleFigure',style={ }),
                    html.Div([
                        dcc.Graph(id=array[2]+'gauge3',figure=arrayFigsOrders[2],style={'background': "#FFFFFF", 'border' : 'none' , 'border-radius' : '1px', 'box-shadow' : '0 1px 1px 0 rgba(70, 76, 79, .2)'})
                    ],className='tripleFigure',style={ })
                ], className="row", style={"margin": "1% 1%"})
        
def mongoAggregateQuery(value):

    return [{'$match': {'number': value}},
    {'$project': {'name': 1,
    'legalName' : 1,
    'cnpj': 1,
    'mainActivity.activity': 1,
    'number': 1,
    'revenueAmount': 1,
    '_p_sector': 1,
    '_p_platformType': 1,
    '_p_segment': 1,
    'employeesNumber': 1,
    'femaleLeadershipPercentual': 1,
    'email' : 1,
    'partnerName' : 1,
    'startedAt' : 1}},
    {'$lookup': {'from': 'Loan',
    'let': {'rootField': '$number'},
    'pipeline': [{'$match': {'$expr': {'$eq': ['$businessNumber',
        '$$rootField']}}},
    {'$sort': {'_created_at': -1}},
    {'$project': {'statusName': 1,
      'portfolioName': 1,
      'leadSource': 1,
      'disbursementDate': 1,
      'amount': 1,
      'totalAmount': 1,
      '_created_at': 1}}],
    'as': 'Loan'}},
    {'$unwind': {'path': '$Loan',
    'includeArrayIndex': 'element',
    'preserveNullAndEmptyArrays': True}},
    {'$addFields': {'element': {'$ifNull': ['$element', 0]}}},
    {'$match': {'element': 0}},
    {'$lookup': {'from': 'Address',
    'let': {'rootField': {'$concat': ['Business$', '$_id']}},
    'pipeline': [{'$match': {'$expr': {'$eq': ['$_p_business',
        '$$rootField']}}},
    {'$sort': {'_created_at': -1}},
    {'$project': {'street': {'$ifNull': ['$street', '']},
      'number': {'$ifNull': ['$number', '']},
      'city': {'$ifNull': ['$city', '']},
      'state': {'$ifNull': ['$state', '']}}}],
    'as': 'Address'}},
    {'$unwind': {'path': '$Address',
    'includeArrayIndex': 'element',
    'preserveNullAndEmptyArrays': True}},
    {'$addFields': {'element': {'$ifNull': ['$element', 0]}}},
    {'$match': {'element': 0}},
    {'$lookup': {'from': 'Score',
    'let': {'rootField': {'$concat': ['Business$', '$_id']}},
    'pipeline': [{'$match': {'$expr': {'$eq': ['$_p_business',
        '$$rootField']}}},
    {'$sort': {'_created_at': -1}},
    {'$project': {'score': 1, 'paymentCapacity': 1, 'score_models' : 1,'anomes' : 1, '_updated_at' : 1}}],
    'as': 'Score'}},
    {'$unwind': {'path': '$Score',
    'includeArrayIndex': 'element',
    'preserveNullAndEmptyArrays': True}},
    {'$addFields': {'element': {'$ifNull': ['$element', 0]}}},
    {'$match': {'element': 0}},
    {'$lookup': {'from': 'IntegrationProScore',
    'let': {'rootField': '$number'},
    'pipeline': [{'$match': {'$expr': {'$eq': ['$businessNumber',
        '$$rootField']}}},
    {'$sort': {'_created_at': -1}},
    {'$match': {'cpf': {'$exists': False}}},
    {'$unwind': {'path': '$summary'}},
    {'$match': {'summary.resultado_final_do_score': {'$exists': True}}},
    {'$project': {'resultado_final_do_score': {'$toInt': '$summary.resultado_final_do_score'}}}],
    'as': 'Bureau'}},
    {'$unwind': {'path': '$Bureau',
    'includeArrayIndex': 'element',
    'preserveNullAndEmptyArrays': True}},
    {'$addFields': {'element': {'$ifNull': ['$element', 0]}}},
    {'$match': {'element': 0}},
    {'$lookup': {'from': 'IntegrationProScore',
    'let': {'rootField': '$number'},
    'pipeline': [{'$match': {'$expr': {'$eq': ['$businessNumber',
        '$$rootField']}}},
    {'$addFields': {'anomes': {'$sum': [{'$multiply': [{'$year': '$_created_at'},
          100]},
        {'$month': '$_created_at'}]}}},
    {'$match': {'cpf': {'$exists': True}}},
    {'$unwind': {'path': '$summary'}},
    {'$match': {'summary.resultado_final_do_score': {'$exists': True}}},
    {'$project': {'anomes': 1,
      'businessNumber': 1,
      'summary.resultado_final_do_score': {'$toInt': '$summary.resultado_final_do_score'}}},
    {'$group': {'_id': {'b': '$businessNumber', 'd': '$anomes'},
      'resultado_final_do_score': {'$avg': '$summary.resultado_final_do_score'}}},
    {'$sort': {'_id.d': -1}}],
    'as': 'BureauCPF'}},
    {'$unwind': {'path': '$BureauCPF',
    'includeArrayIndex': 'element',
    'preserveNullAndEmptyArrays': True}},
    {'$addFields': {'element': {'$ifNull': ['$element', 0]}}},
    {'$match': {'element': 0}}]
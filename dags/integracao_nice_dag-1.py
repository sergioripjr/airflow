# importando libraries
import requests 
import pandas as pd
from datetime import timedelta, date, datetime
import numpy as np
import isodate
from google.cloud import bigquery
from dateutil import parser

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Ignore Warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Inicia Client BigQuery
client = bigquery.Client(project='lclz-dados')

def get_start_date(ti):
    """ 
    Function para retornar os dados de DataInicial
    Data de hoje com o horario do comeco do dia
    """
    # Data hoje
    dt = datetime.today()

    # Start Date
    day_start = dt.day
    month_start = dt.month
    year_start = dt.year
    hour_start = '00:00:00'
    start_date = '{}/{}/{} {}'.format(day_start, month_start, year_start, hour_start)
    start_date = datetime.strptime(start_date, '%d/%m/%Y %H:%M:%S' )
    start_date = start_date.isoformat() + 'Z'
    print(f'Start date: {start_date}')

    # XCOM para dar o push da string de data
    ti.xcom_push(key='start_date', value=start_date)

def get_end_date(ti):
    """ 
    Function para retornar os dados de DataFinal 
    Retorna o horario atual
    """

    # Data hoje
    dt = datetime.today()

    # End Date
    # UTC + 0 
    dt_end = dt
    day_end = dt_end.day
    month_end = dt_end.month
    year_end = dt_end.year
    hour_end = f'{dt_end.hour}:{dt_end.minute}:{dt_end.second}'
    end_date = '{}/{}/{} {}'.format(day_end, month_end, year_end, hour_end)
    end_date = datetime.strptime(end_date, '%d/%m/%Y %H:%M:%S' )
    end_date = end_date.isoformat() + 'Z'
    print(f'End date: {end_date}')

    # XCOM para dar o push da string de data
    ti.xcom_push(key='end_date', value=end_date)

def get_token(ti):
    """ 
    Function para retornar a Token de Acesso
    Recebe o body com as credenciais de acesso:
    id & secret
    """

    url_token = 'https://na1.nice-incontact.com/authentication/v1/token/access-key'
    content_type = 'application/json'

    # Body Authenticate
    body = {

                "accessKeyId": "ZSQZ63K7AZC5SDEUWM6PRBDVAE37NB4H3K7QAXTR4Q7VTAJDPPCQ====",

                "accessKeySecret": "HKRVQAOOCRK7X4Q6PJO5URIAXCBZEVZKRUTFBBZMAC5I7JZUWHMA===="

                }
    
    result_token = requests.post(url_token, verify=False, headers={'Content-Type': content_type}, json=body).json()
    result_token = result_token['access_token']   

    # XCOM para dar o push da string de Token
    ti.xcom_push(key='token', value=result_token) 

def get_skills_summary(ti):
    """
    Function para retornar a lista de Summary de cada Skills
    Agrupado
    Utiliza oAuth Bearer (Token)
    access_token >> Bearer (header)
    Utiliza do start date e end date do dia atual
    """
    
    # Inicializa XCOMS
    auth_token = ti.xcom_pull(key='token')
    start_date = ti.xcom_pull(key='start_date')
    end_date = ti.xcom_pull(key='end_date')

    # ETL
    def df_transform_load(df):

        # dt_registro
        df['dt_registro'] = datetime.today() - timedelta(hours=3)

        # Colunas para int
        columns_int = [
            'businessUnitId',
            'abandonCount',
            'abandonRate',
            'agentsAcw',
            'agentsAvailable',
            'agentsIdle',
            'agentsLoggedIn',
            'agentsUnavailable',
            'agentsWorking',
            'campaignId',
            'contactsActive',
            'contactsHandled',
            'contactsOffered',
            'contactsQueued',
            'contactsOutOfSLA',
            'contactsWithinSLA',
            'mediaTypeId',
            'queueCount',
            'serviceLevel',
            'skillId',
            'serviceLevelGoal',
            'serviceLevelThreshold',
            'dials',
            'connects',
        ]

        for c in columns_int:
            df[c] = df[c].fillna(-1).astype(int)

        # Ajustando colunas de Periodo ISO 8601 para Datetime
        # Transformando todas em String
        columns = [
            'averageHandleTime',
            'averageInqueueTime',
            'averageSpeedToAnswer',
            'averageTalkTime',
            'averageWrapTime',
            'holdTime',
            'longestQueueDur',
            'totalContactTime',
            'rightPartyConnectsAHT',
            'connectsAHT'
        ]

        for i in columns:
            df[i] = df[i].astype(str)

        # Ajustando Cada linha das colunas para Datetime e convertendo
        for c in columns:
            for i in range(len(df[c])):
                df[c][i] = str(isodate.parse_duration(df[c][i]))

        print('Começo da Carga para o BQ')
        # Realizando carga bq
        try:
            df.to_gbq(
                destination_table='nice.nice_ra_skills_summary',
                if_exists='append')
            print('Carga Finalizada \n')
        except Exception as e:
            print(e)

    # SQL para pegar a ultima data de atualizacao da tabela
    sql = '''
    SELECT
            max(c1.dt_registro)  as ultima_att
    FROM `nice.nice_ra_skills_summary` c1
    '''

    # DF Com ultima data att
    # Transformacao para selecionar ultima data de carga
    df = client.query(sql).to_dataframe()
    ultima_att = df['ultima_att'][0].isoformat()
    ultima_att = parser.parse(ultima_att)

    # Data ultima atualizacao para delete
    ultima_delete = ultima_att.strftime('%Y-%m-%d')
    print(f'Ultima data de atualizacao na tabela: {ultima_delete}')

    # Data de hoje
    hoje_delete = datetime.today() - timedelta(hours=3)
    hoje_delete = hoje_delete.strftime('%Y-%m-%d')
    print(f'Data de hoje: {hoje_delete} \n' )

    # Logica para UPSERT
    if ultima_delete == hoje_delete:

        # Deleta os dados para realizar o insert
        try: 
            query_delete = f'''
            DELETE FROM `lclz-dados.nice.nice_ra_skills_summary`
            where date(dt_registro) >= '{hoje_delete}'
            '''

            print('\nDeletando dados para o upsert:')
            print(query_delete)
            job = client.query(query_delete)
            job.result()
            print(f'Dados do dia {hoje_delete} deletados! \n')
        except Exception as e:
            print(e)
        
        print('Inserindo novos dados na tabela (UPSERT)! \n')
        url_skills = f'https://api-c53.nice-incontact.com/incontactapi/services/v24.0/skills/summary?startDate={start_date}&endDate={end_date}'
        headers = {'Authorization': 'Bearer ' + auth_token,
                    'Content-Type': 'application/json'}
        
        
        result_skills = requests.get(url_skills, verify=False, headers=headers).json()
        result_json = result_skills['skillSummaries']
        df = pd.DataFrame(result_json)

        # Transform and Load to BQ
        df_transform_load(df)
    
    # Caso a ultima data de atualizacao seja menor que o dia de hoje ele realiza apenas o insert
    else:
        
        print(f'\nData da tabela {ultima_delete} menor do que a data de hoje {end_date}')
        
        url_skills = f'https://api-c53.nice-incontact.com/incontactapi/services/v24.0/skills/summary?startDate={start_date}&endDate={end_date}'
        headers = {'Authorization': 'Bearer ' + auth_token,
                    'Content-Type': 'application/json'}
        
        result_skills = requests.get(url_skills, verify=False, headers=headers).json()
        result_json = result_skills['skillSummaries']

        # Inicializando o DataFrame
        df = pd.DataFrame(result_json)

        # Transform and Load to BQ
        df_transform_load(df)
        print('Insert Realizado com sucesso!')
    
def get_contacts_completed(ti):
    """
    Function para retornar a lista agrupado de cada contato (ligação) completa
    Utilização de datas na ISO 8601 para os parametros de dada
    Utiliza oAuth Bearer (Token)
    access_token >> Bearer (header)
    Utiliza da Logica de MAX date para Particionar as consultas
    """

    # Inicializa o Token
    auth_token = ti.xcom_pull(key='token')

    # Start_date
    # Query max_date
    sql = '''
    SELECT
            max(c1.ultima_atualizacao)  as ultima_att
    FROM `nice.nice_ra_contacts_complete` c1
    '''
    
    # DF Com ultima data att
    df = client.query(sql).to_dataframe()
    ultima_att = df['ultima_att'][0]
    ultima_att = parser.parse(ultima_att)

    # Adicionando 1 sec para nao duplicar dados da ultima chamada
    ultima_att = ultima_att + timedelta(seconds=1)

    # Formatando data
    day_start = ultima_att.day
    month_start = ultima_att.month
    year_start = ultima_att.year
    hour_start = f'{ultima_att.hour}:{ultima_att.minute}:{ultima_att.second}'

    start_date = '{}/{}/{} {}'.format(day_start, month_start, year_start, hour_start)
    start_date = datetime.strptime(start_date, '%d/%m/%Y %H:%M:%S' )
    start_date = start_date.isoformat() + 'Z'
    print(f'Start date: {start_date}' )

    # End_date
    # Com base no horario atual
    now = datetime.now()

    # Formatando data
    day_end = now.day
    month_end = now.month
    year_end = now.year
    hour_end = f'{now.hour}:{now.minute}:{now.second}'

    end_date = '{}/{}/{} {}'.format(day_end, month_end, year_end, hour_end)
    end_date = datetime.strptime(end_date, '%d/%m/%Y %H:%M:%S' )
    end_date = end_date.isoformat() + 'Z'
    print(f'End Date: {end_date}' )

    url_contacts = f'https://api-c53.nice-incontact.com/incontactapi/services/v24.0/contacts/completed?startDate={start_date}&endDate={end_date}'
    headers = {'Authorization': 'Bearer ' + auth_token,
            'Content-Type': 'application/json'}

    result_contacts = requests.get(url_contacts, verify=False, headers=headers).json()
    result_json = result_contacts['completedContacts']

    # Criacao logica paginacao
    result_contacts = requests.get(url_contacts, verify=False, headers=headers).json()
    result_json_size = result_contacts['totalRecords']
    total_interation_skip = int(str(result_json_size / 10000)[:1])

    # Inicializando o DataFrame
    df = pd.DataFrame(result_json)

    # Logica paginacao 
    # Criacao dataframe com as paginas da api
    if result_json_size >= 10000:
        print(f'Dataset maior que 10k Registros, total rows: {result_json_size}')
        
        for i in range(total_interation_skip):
            y = 1 + i 
            j = 10000 * y
            result_contacts = requests.get(url_contacts+'&skip={}'.format(j), verify=False, headers=headers).json()
            df_1 = pd.DataFrame(result_contacts['completedContacts'])
            df = df.append(df_1, ignore_index=True)

    # Coluna de Ultima att
    df['ultima_atualizacao'] = end_date

    # Columns to Date
    columns = [
        'contactStart', 
        'dateContactWarehoused', 
        'lastUpdateTime', 
        'dateACWWarehoused',
    ]

    # Ajustando Datas para Datetime
    for c in columns: 
        df[c] = pd.to_datetime(df[c]).dt.strftime('%Y-%m-%d %H:%M:%S')
        for i in columns:
            df[i] = pd.to_datetime(df[i])

    # Subtraindo horas para UTC - 3 (Br)
    for j in columns:
        df[j] = pd.to_datetime(df[j].astype(str)) - pd.DateOffset(hours=3)

    # Coluna string
    df['mediaSubTypeId'] = df['mediaSubTypeId'].astype(str)

    # Columns to int
    columns_int = [
        'agentId', 
        'primaryDispositionId', 
        'secondaryDispositionId', 
        'teamId',
        'contactId',
        'campaignId',
        'callbackTime',
        'holdCount',
        'masterContactId',
        'mediaType',
        'pointOfContactId',
        'routingTime',
        'serviceLevelFlag',
        'skillId',
        'transferIndicatorId'
        ]

    for c in columns_int:
        df[c] = df[c].fillna(-1).astype(np.int64)

    # Columns to float
    columns_float = [ 
        'abandonSeconds', 
        'agentSeconds', 
        'holdSeconds', 
        'inQueueSeconds', 
        'postQueueSeconds', 
        'preQueueSeconds', 
        'releaseSeconds',
        'confSeconds',
        'totalDurationSeconds'
        ]

    for c in columns_float:
        df[c] = df[c].replace(',', '', regex=True)

    for i in columns_float:
        df[i] = df[i].astype(float)

    # Float acw
    df['acwSeconds'] = df['acwSeconds'].replace(',', '', regex=True)
    df['acwSeconds'] = df['acwSeconds'].astype(float)
    
    # Droping column 'tags'
    try:
        df.drop('tags', axis=1, inplace=True)
    except:
        pass

    # Realizando carga bq
    try:
        df.to_gbq(
        destination_table='nice.nice_ra_contacts_complete',
        if_exists='append')

        print(f'Total de linhas inseridas no GCP: {result_json_size} \n')
    except Exception as e:
        print(e)


def get_sla_summary(ti):
    """
    Function para retornar a lista de Summary de cada Skills
    Agrupado
    Utiliza oAuth Bearer (Token)
    access_token >> Bearer (header)
    """
    
    # Inicializa XCOMS
    auth_token = ti.xcom_pull(key='token')
    start_date = ti.xcom_pull(key='start_date')
    end_date = ti.xcom_pull(key='end_date')

     # SQL para pegar a ultima data de atualizacao da tabela
    sql = '''
    SELECT
            max(c1.dt_registro)  as ultima_att
    FROM `nice.nice_ra_sla_summary` c1
    '''
    
    # DF Com ultima data att
    # Transformacao para selecionar ultima data de carga
    df = client.query(sql).to_dataframe()
    ultima_att = df['ultima_att'][0]
    ultima_att = parser.parse(ultima_att)

    # Data ultima atualizacao para delete
    # UPSERT
    ultima_delete = ultima_att.strftime('%Y-%m-%d')
    print(f'Ultima data de atualizacao na tabela: {ultima_delete}')

    # Data de hoje
    hoje_delete = datetime.today().strftime('%Y-%m-%d')
    print(f'Data de hoje: {hoje_delete} \n' )
    
    # logica para UPSERT
    if ultima_delete == hoje_delete:

        # Deleta os dados para realizar o insert
        try: 
            query_delete = f'''
            DELETE FROM `lclz-dados.nice.nice_ra_sla_summary`
            where date(dt_registro) >= '{hoje_delete}'
            '''

            print('\nDeletando dados para o upsert:')
            print(query_delete)
            job = client.query(query_delete)
            job.result()
            print(f'Dados do dia {hoje_delete} deletados! \n')
        except Exception as e:
            print(e)
        
        print('Inserindo novos dados na tabela (UPSERT)! \n')
        url_sla = f'https://api-c53.nice-incontact.com/incontactapi/services/v24.0/skills/sla-summary?startDate={start_date}&endDate={end_date}'
        headers = {'Authorization': 'Bearer ' + auth_token,
                    'Content-Type': 'application/json'}

        result_sla = requests.get(url_sla, verify=False, headers=headers).json()
        result_json = result_sla['serviceLevelSummaries']
        df = pd.DataFrame(result_json)

        # Adiciona dt_registro
        df['dt_registro'] = datetime.today().strftime('%Y-%m-%d')

         # Realizando carga bq
        try:
            df.to_gbq(

                destination_table='nice.nice_ra_sla_summary',
                if_exists='append')
            print('Carga Finalizada \n')
        except Exception as e:
            print(e)
    
    # Caso a ultima data de atualizacao seja menor que o dia de hoje ele realiza apenas o insert
    else:
        
        print(f'\nData da tabela {ultima_delete} menor do que a data de hoje {end_date}')
        
        url_skills = f'https://api-c53.nice-incontact.com/incontactapi/services/v24.0/skills/sla-summary?startDate={start_date}&endDate={end_date}'
        headers = {'Authorization': 'Bearer ' + auth_token,
                    'Content-Type': 'application/json'}
        
        result_sla = requests.get(url_skills, verify=False, headers=headers).json()
        result_json = result_sla['serviceLevelSummaries']

        # Inicializando o DataFrame
        df = pd.DataFrame(result_json)

        # Adiciona dt_registro
        df['dt_registro'] = datetime.today().strftime('%Y-%m-%d')
        
        # Realizando carga bq
        try:
            df.to_gbq(
                destination_table='nice.nice_ra_sla_summary',
                if_exists='append')
            print('Carga Finalizada \n')
        except Exception as e:
            print(e)

        # Transform and Load to BQ
        print('Insert Realizado com sucesso!')

def get_agents_performance(ti):
    """
    Function para retornar a performance de cada agente
    Utilização de datas na ISO 8601 para os parametros de dada
    Utiliza oAuth Bearer (Token)
    (UPSERT)
    access_token >> Bearer (header)
    """

    # Inicializa XCOMS
    auth_token = ti.xcom_pull(key='token')

    def start_date():
        # Data hoje
        dt = datetime.today()

        # Start Date
        start_date = dt.strftime('%Y-%m-%d')
        print(f'Start Date: {start_date}')
        return start_date

    def end_date():
        # Data hoje
        dt = datetime.today()

        # End Date
        # UTC + 0 
        end_date = dt.strftime('%Y-%m-%d')
        print(f'End date: {end_date}')
        return end_date

    def df_transform_load(df):
        # dt_registro
        df['dt_registro'] = datetime.today() - timedelta(hours=3)

        # Colunas periodo para Time Delta
        columns = [
            'inboundTime',
            'inboundTalkTime',
            'inboundAvgTalkTime',
            'outboundTime',
            'outboundTalkTime',
            'outboundAvgTalkTime',
            'totalTalkTime',
            'totalAvgTalkTime',
            'totalAvgHandleTime',
            'consultTime',
            'availableTime',
            'unavailableTime',
            'acwTime',
            'loginTime'
        ]

        # Ajustando colunas de Periodo ISO 8601 para Datetime
        # Transformando todas em String
        for i in columns:
            df[i] = df[i].astype(str)

        # Ajustando Cada linha das colunas para Datetime e convertendo
        for c in columns:
            for i in range(len(df[c])):
                df[c][i] = str(isodate.parse_duration(df[c][i]))

        # Realizando carga bq
        try:
            df.to_gbq(
                destination_table='nice.nice_ra_agents_performance',
                if_exists='append')
            print('Carga Finalizada! \n')
        except Exception as e:
            print(e)

    # SQL para pegar a ultima data de atualizacao da tabela
    sql = '''
    SELECT
        MAX(c.dt_registro)      as ultima_att
    FROM `nice.nice_ra_agents_performance`  c 
    '''
    
    # DF Com ultima data att
    # Transformacao para selecionar ultima data de carga
    df = client.query(sql).to_dataframe()
    ultima_att = df['ultima_att'][0].isoformat()
    ultima_att = parser.parse(ultima_att)

    # Data ultima atualizacao para delete
    # UPSERT
    ultima_delete = ultima_att.strftime('%Y-%m-%d')
    print(f'Ultima data de atualizacao na tabela: {ultima_delete}')

    # Data de hoje
    hoje_delete = datetime.today().strftime('%Y-%m-%d')
    print(f'Data de hoje: {hoje_delete} \n' )

    # Logica Upsert
    if ultima_delete == hoje_delete:
        
        # Deleta os dados para realizar o insert
        try: 
            query_delete = f'''
            DELETE FROM `nice.nice_ra_agents_performance`
            where date(dt_registro) >= '{hoje_delete}'
            '''
            
            start_date_ = start_date()
            end_date_ = end_date()

            print('\nDeletando dados para o upsert:')
            print(query_delete)
            job = client.query(query_delete)
            job.result()
            print(f'Dados do dia {hoje_delete} deletados! \n')
        except Exception as e:
            print(e)

        print('Inserindo novos dados na tabela (UPSERT)! \n')
        url_agents = f'https://api-c53.nice-incontact.com/incontactapi/services/v24.0/agents/performance?startDate={start_date_}&endDate={end_date_}'
        headers = {'Authorization': 'Bearer ' + auth_token,
                'Content-Type': 'application/json'}

        result_agents = requests.get(url_agents, verify=False, headers=headers).json()
        result_json = result_agents['agentPerformance']
        df = pd.DataFrame(result_json)

        # Transform and Load to BQ
        df_transform_load(df)

    # Caso a ultima data de atualizacao seja menor que o dia de hoje ele realiza apenas o insert
    else:
        start_date_ = start_date()
        end_date_ = end_date()
        print(f'\nData da tabela {ultima_delete} menor do que a data de hoje {end_date_}')
        
        url_agents = f'https://api-c53.nice-incontact.com/incontactapi/services/v24.0/agents/performance?startDate={start_date_}&endDate={end_date_}'
        headers = {'Authorization': 'Bearer ' + auth_token,
                'Content-Type': 'application/json'}

        result_agents = requests.get(url_agents, verify=False, headers=headers).json()
        result_json = result_agents['agentPerformance']
        df = pd.DataFrame(result_json)

        # Inicializando o DataFrame
        df = pd.DataFrame(result_json)

        # Transform and Load to BQ
        df_transform_load(df)
        print('Insert Realizado com sucesso!')

# Ajuste e configuracao da DAG para o Airflow
default_args = {
    'start_date': '2022-08-02',
    'retries': 1,
    'owner': 'Squad Jupyter',
    'retry_delay': timedelta(seconds=30)
}        

with DAG (dag_id='nice_integracao_reporting_api',
         description='Realiza a carga da API (Reporting) da Nice para o BigQuery',
         schedule_interval='30 * * * *',
         default_args=default_args,
         concurrency=20,
         max_active_runs=1,
         tags=['Nice',"SquadJupyter", "CERES"],
         catchup=False 
) as dag:

    task_start = DummyOperator(
        task_id='start'
    )

    start_date = PythonOperator(
        task_id = 'start_date',
        python_callable = get_start_date,
        provide_context = True,
        dag = dag
    ) 

    end_date = PythonOperator(
        task_id = 'end_date',
        python_callable = get_end_date,
        provide_context = True,
        dag = dag
    )

    token = PythonOperator(
        task_id = 'get_api_token',
        python_callable = get_token,
        provide_context = True,
        dag = dag
    )

    skills_summary = PythonOperator(
        task_id = 'skills_summary',
        python_callable = get_skills_summary,
        provide_context = True,
        dag = dag
    )

    contacts_complete = PythonOperator(
        task_id = 'contacts_completed',
        python_callable = get_contacts_completed,
        provide_context = True,
        dag = dag
    )

    sla_summary = PythonOperator(
        task_id = 'sla_summary',
        python_callable = get_sla_summary,
        provide_context = True,
        dag = dag
    )

    agents_performance = PythonOperator(
        task_id = 'agents_performance',
        python_callable = get_agents_performance,
        provide_context = True,
        dag = dag
    )

    task_end = DummyOperator(
        task_id='end'
    )

    task_start >> [start_date, end_date, token] >> skills_summary >> contacts_complete >> sla_summary >> agents_performance >> task_end
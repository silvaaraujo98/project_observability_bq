import pandas as pd 
import numpy as np
from aux_functions import *
import time

def ReadParquetDataset(path):
    df = pd.read_parquet(path)
    return df



def ReadBigQueryDataset():
    
    project_id = "data-core-platform"  # Substitua pelo ID do seu projeto
    dataset_id = "SINK_riogaleao_com_br"      # Substitua pelo ID do seu dataset
    table_id = "vw_bigquery_data_access"  
    print("Iniciando a Query")
    start_time = time.time() # Registra o tempo inicial
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` ORDER BY `Date` LIMIT 250000 "
    df = extract('data-core-platform',query)
    end_time = time.time() # Registra o tempo final
    
    print("Finalizando a Query")
    elapsed_time = end_time - start_time
    print(f"Tempo decorrido para a query: {elapsed_time:.2f} segundos")
    
    df_copy = df.copy()
    df_copy.to_parquet("./data/sample_metadata_bigquery.parquet")

    return df_copy


def SetColumnsDate(df):

    df['date'] = pd.to_datetime(df['Date'])
    df.drop('Date',inplace=True)
    df['JobStartTime'] = pd.to_datetime(df['JobStartTime'])
    df['JobEndTime'] = pd.to_datetime(df['JobEndTime'])

    return df



def CreateColumnClusterTime(df):

    def CreateClusterTime(date_time):
        # Calcula o número de minutos desde o começo da hora
        minutes = date_time.minute
        # Calculate the floor division by 5 to get the correct 5-minute interval.
        # Calcula o piso da divisão por 5 para obter o intervalor correto de 5 minutos
        minute_cluster = (minutes // 5) * 5
        # Substitui os minutos e segundos pelo cluster calculado pela divisão com zero segundos
        clustered_time = date_time.replace(minute=minute_cluster, second=0, microsecond=0)
        return clustered_time
    
    df['clusterized_date'] = df['date'].apply(CreateClusterTime)

    return df

def create_execution_time(df):

    df['execution_time_min'] = (((df['JobEndTime'] - df['JobStartTime'])).dt.total_seconds())/60

    return df

    

def run_all_transformation_functions():

    #df = ReadBigQueryDataset()
    df = ReadParquetDataset("data\sample_metadata_bigquery.parquet")
    df_transformed = df.pipe(SetColumnsDate)\
    .pipe(CreateColumnClusterTime)\
    .pipe(create_execution_time)

    
    return df_transformed


if __name__ == '__main__':
    run_all_transformation_functions()
import pandas as pd 
import numpy as np

def ReadParquet(path):

    df = pd.read_parquet(path)
    df_copy = df.copy()

    return df_copy


def SetColumnsDate(df):

    df['Date'] = pd.to_datetime(df['Date'])
    df['JobStartTime'] = pd.to_datetime(df['JobStartTime'])
    df['JobEndTime'] = pd.to_datetime(df['JobEndTime'])

    return df

def TransformingSlottoMin(df):

    df['TotalSlotMin'] = df['TotalSlotMs']/60000

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
    
    df['Clusterized_Date'] = df['Date'].apply(CreateClusterTime)

    return df

def create_execution_time(df):
    df['execution_time_min'] = (((df['JobEndTime'] - df['JobStartTime'])).dt.total_seconds())/60
    return df

def run_all_transformation_functions():
    path = "./data/sample_metadata_bigquery.parquet"
    df = pd.read_parquet(path)
    df_transformed = df.pipe(SetColumnsDate)\
    .pipe(TransformingSlottoMin)\
    .pipe(CreateColumnClusterTime)\
    .pipe(create_execution_time)
    
    return df_transformed

    
if __name__ == '__main__':
    run_all_transformation_functions()
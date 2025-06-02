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


def CreateClusterTime(date_time):
           '''
    Cria um cluster de tempo de 5 minutos para uma data qualquer.

    Args:
        date_time(pd.Timestamp) : A data para ser clusterizada

    Returns:
        pd.Timestamp: A data arrendoda para baixa com a marca de 5 minutos mais próxima.
    '''
    # Calcula o número de minutos desde o começo da hora
    minutes = date_time.minute
    # Calculate the floor division by 5 to get the correct 5-minute interval.
    # Calcula o piso da divisão por 5 para obter o intervalor correto de 5 minutos
    minute_cluster = (minutes // 5) * 5
    # Substitui os minutos e segundos pelo cluster calculado pela divisão com zero segundos
    clustered_time = date_time.replace(minute=minute_cluster, second=0, microsecond=0)
    return clustered_time
    
if __name__ == '__main__':
    ReadParquet("./data/sample_metadata_bigquery.parquet")
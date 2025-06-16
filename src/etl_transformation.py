import pandas as pd 
import numpy as np

def ReadParquet(path):

    df = pd.read_parquet(path)
    df_copy = df.copy()
    df_copy.rename(columns={'ProjectId':"project_id","Queries":"queries_perfomed","Date":"date"},inplace=True)

    return df_copy


def SetColumnsDate(df):

    df['date'] = pd.to_datetime(df['date'])
    df['JobStartTime'] = pd.to_datetime(df['JobStartTime'])
    df['JobEndTime'] = pd.to_datetime(df['JobEndTime'])

    return df

def TransformingSlottoMin(df):

    df['total_slot_min'] = df['TotalSlotMs']/60000

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

    path = "./data/sample_metadata_bigquery.parquet"
    df = ReadParquet(path)

    df_transformed = df.pipe(SetColumnsDate)\
    .pipe(TransformingSlottoMin)\
    .pipe(CreateColumnClusterTime)\
    .pipe(create_execution_time)\
    .pipe(grouping_dataframe)\
    .pipe(shift_dataframe)
    
    return df_transformed

def grouping_dataframe(df):
    grouped_df = df.groupby(['clusterized_date','project_id']).agg({"queries_perfomed":"sum","execution_time_min":"mean","total_slot_min":"sum"}).reset_index()
    return grouped_df


def shift_dataframe(df):
    df['clusterized_date_24h_ago'] = df['clusterized_date'] - pd.Timedelta(days=1)
    df_shifted = df[['clusterized_date','project_id','queries_perfomed','execution_time_min','total_slot_min']]\
        .rename(columns={'clusterized_date': 'clusterized_date_24h_ago', 
                         'queries_perfomed': 'queries_perfomed_24h_ago',
                         'execution_time_min': 'execution_time_min_24h_ago',
                         'total_slot_min': 'total_slot_min_24h_ago'})
    df_merged = pd.merge(df, df_shifted, on=['clusterized_date_24h_ago','project_id'], how='left')
    return df


if __name__ == '__main__':
    run_all_transformation_functions()
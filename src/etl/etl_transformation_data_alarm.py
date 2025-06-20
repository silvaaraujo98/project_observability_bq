from src.etl.etl_transformation import run_all_transformation_functions
import pandas as pd


def read_threshold():

    df_threshold_executiontime = pd.read_csv("./data/Data Services - Levantamento de Thresholds - Tempo Med. Execução.csv")
    df_threshold_queries = pd.read_csv("./data/Data Services - Levantamento de Thresholds - Consultas Realizadas.csv")
    
    df_threshold_executiontime['threshold_executiontime'] = df_threshold_executiontime['Threshold'].str.replace("min","").astype(float)
    df_threshold_queries['threshold_queries'] = df_threshold_queries['Threshold'].astype(float)

    df_threshold_queries.drop('Threshold',axis=1,inplace=True)
    df_threshold_executiontime.drop('Threshold',axis=1,inplace=True)

    return df_threshold_executiontime, df_threshold_queries


def merging_threshold_dataframes(df_threshold_executiontime,df_threshold_queries):

    df_threshold = pd.merge(df_threshold_executiontime,df_threshold_queries,on='Project ID')
    
    return df_threshold

def read_bq_metadata():

    df_bq_metadata = run_all_transformation_functions()

    return df_bq_metadata


def get_last_values_bq_metadata(df_bq_metadata):

    df_last_values_bq_metadata = df_bq_metadata[df_bq_metadata['Clusterized_Date'] == df_bq_metadata['Clusterized_Date'].max()]
    
    return df_last_values_bq_metadata

def group_queries_executiontime_project(df_last_values_bq_metadata):

    df_grouped_executiontime_queries = df_last_values_bq_metadata.groupby("ProjectId").agg({'execution_time_min':'mean'
                                                                                            ,'Queries':'sum','Clusterized_Date':'max'}).reset_index()
    return df_grouped_executiontime_queries

def merge_bqmetadata_threshold(df_threshold):
    df_grouped_executiontime_queries = read_bq_metadata().pipe(get_last_values_bq_metadata).pipe(group_queries_executiontime_project)
    
    grouped_exeuctiontime_queries_threshold_df = pd.merge(
        df_grouped_executiontime_queries,
             df_threshold,
             left_on='ProjectId',
             right_on='Project ID',
             how='inner')
    
    
    return grouped_exeuctiontime_queries_threshold_df
    

def create_conditional_columns_to_send_email(grouped_exeuctiontime_queries_threshold_df):
    
    grouped_exeuctiontime_queries_threshold_df['execution_time_send_email_flag'] = grouped_exeuctiontime_queries_threshold_df['threshold_executiontime'] > grouped_exeuctiontime_queries_threshold_df['threshold_executiontime']
    grouped_exeuctiontime_queries_threshold_df['queries_send_email_flag'] = grouped_exeuctiontime_queries_threshold_df['Queries'] > grouped_exeuctiontime_queries_threshold_df['threshold_queries']

    return grouped_exeuctiontime_queries_threshold_df

def filter_only_true_thresholdcolumn(df):

    filtered_df = df[(df['queries_send_email_flag'] | df['execution_time_send_email_flag'])]

    return filtered_df


def run_all_transformation_data():
   df_threshold_executiontime, df_threshold_queries = read_threshold()
   df = merging_threshold_dataframes(df_threshold_executiontime,df_threshold_queries)
   df_merged = merge_bqmetadata_threshold(df)
   df_conditional = create_conditional_columns_to_send_email(df_merged)
   filtered_df = filter_only_true_thresholdcolumn(df_conditional)


   return filtered_df




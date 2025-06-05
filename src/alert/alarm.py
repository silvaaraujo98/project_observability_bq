from data_processing.transformation import run_all_transformation_functions
import pandas as pd

df_bq_metadata = run_all_transformation_functions()

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


def get_last_values_bq_metadata(df_bq_metadata):

    df_last_values_bq_metadata = df_bq_metadata[df_bq_metadata['Clusterized_Date'] == df_bq_metadata['Clusterized_Date'].max()]
    
    return df_last_values_bq_metadata

def group_queries_executiontime_project(df_last_values_bq_metadata):

    df_grouped_executiontime_queries = df_last_values_bq_metadata.groupby("ProjectId").agg({'execution_time_min':'mean'
                                                                                            ,'Queries':'sum','Clusterized_Date':'max'}).reset_index()
    return df_grouped_executiontime_queries

def merge_bqmetadata_threshold(df_grouped_executiontime_queries,df_threshold):
    
    grouped_exeuctiontime_queries_threshold_df = pd.merge(
        df_grouped_executiontime_queries,
             df_threshold,
             left_on='ProjectId',
             right_on='Project ID',
             how='inner')
    
    
    return grouped_exeuctiontime_queries_threshold_df
    

def create_conditional_columns_to_send_email(grouped_exeuctiontime_queries_threshold_df):
    
    grouped_exeuctiontime_queries_threshold_df['execution_time_send_email_flag'] = grouped_exeuctiontime_queries_threshold_df['execution_time_min'] > grouped_exeuctiontime_queries_threshold_df['threshold_executiontime']
    grouped_exeuctiontime_queries_threshold_df['queries_send_email_flag'] = grouped_exeuctiontime_queries_threshold_df['Queries'] > grouped_exeuctiontime_queries_threshold_df['threshold_queries']

    return grouped_exeuctiontime_queries_threshold_df






def monitor_bq_projects(df_bq_metadata, df_thresholds):
    """
    Monitors BigQuery project execution time and query count,
    triggering email alerts if configured thresholds are exceeded.

    This function compares the latest BigQuery project execution metadata records
    with predefined thresholds. If a project's total execution time or total
    number of queries surpasses its configured limits, an alert email is sent
    to the specified recipients.

    Args:
        df_bq_metadata (pd.DataFrame): DataFrame containing the latest BigQuery
            project execution metadata records.
            Expected columns (example):
            - 'project_id' (str): BigQuery project ID.
            - 'total_execution_time_seconds' (float): Total execution time in seconds.
            - 'total_queries_executed' (int): Total number of queries executed.
            - 'last_update_timestamp' (datetime): Timestamp of the last record update.
            (Assumes this DataFrame is already consolidated with the latest record per project)

        df_thresholds (pd.DataFrame): DataFrame containing the alert thresholds
            for each project.
            Expected columns (example):
            - 'project_id' (str): BigQuery project ID.
            - 'max_execution_time_seconds' (float): Maximum execution time threshold in seconds.
            - 'max_queries_executed' (int): Maximum query count threshold.
            - 'alert_recipients' (str): Comma-separated string of recipient emails.
            - 'is_monitoring_enabled' (bool): Flag to indicate if monitoring is enabled for the project.

    Returns:
        None: The function does not explicitly return any value, but it performs
              the action of sending emails when thresholds are exceeded.
    """
    # (Function implementation would go here)
    # 1. Validate DataFrames and columns
    # 2. Merge DataFrames
    # 3. Iterate over monitored projects
    # 4. Check thresholds
    # 5. Compose and send email


    pass # Placeholder for the implementation


if __name__ == '__main__':
    df_threshold_executiontime, df_threshold_queries = read_threshold()
    df_threshold = merging_threshold_dataframes(df_threshold_executiontime, df_threshold_queries)
    df_grouped_executiontime_queries = get_last_values_bq_metadata(df_bq_metadata).pipe(group_queries_executiontime_project)
    grouped_exeuctiontime_queries_threshold_df = merge_bqmetadata_threshold(df_grouped_executiontime_queries,df_threshold)
    grouped_exeuctiontime_queries_threshold_df = create_conditional_columns_to_send_email(grouped_exeuctiontime_queries_threshold_df)
    


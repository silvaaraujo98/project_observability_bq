from alert_send_email import iterating_dataframe_to_write_email
from etl_transformation_data_alarm import run_all_transformation_data



if __name__ =='__main__':
    df = run_all_transformation_data()
    iterating_dataframe_to_write_email(df)

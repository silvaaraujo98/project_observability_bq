import datetime
def get_specific_columns(df,*columns_to_select):

  return df[list(columns_to_select)]

def group_and_aggregate_data(df, numerical_column, *categorical_columns, aggregation_method='sum'):
  
  grouped_df = df.groupby(list(categorical_columns))[numerical_column].agg(aggregation_method).reset_index()
  
  return grouped_df


def get_value_columns_in_hours(df,date_column,value_column,hours=24,agg_func='sum'):
  max_column_date = max(df[date_column])
  df_filtered = df[(df[date_column]<=max_column_date) & (df[date_column] > max_column_date - datetime.timedelta(hours=hours))]
  agg_func_value_column = df_filtered[value_column].agg(agg_func)
  return agg_func_value_column

## test


def get_specific_columns(df,*columns_to_select):

  return df[list(columns_to_select)]

def group_and_aggregate_data(df, numerical_column, *categorical_columns, aggregation_method='sum'):
  
  grouped_df = df.groupby(list(categorical_columns))[numerical_column].agg(aggregation_method).reset_index()
  
  return grouped_df
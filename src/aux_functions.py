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
import locale


def format_br_number(value):
  """
  Formata um valor numérico para o padrão brasileiro:
  milhares com ponto, decimais com vírgula.

  Exemplos:
  14560.27 -> '14.560,27'
  1234567 -> '1.234.567'
  1000.00 -> '1.000,00'

  Args:
    value (int ou float): O número a ser formatado.

  Returns:
    str: O número formatado como string no padrão brasileiro.
  """
  # Tenta configurar a localidade para Português do Brasil
  # Há diferentes nomes para a localidade dependendo do sistema operacional.
  # pt_BR.UTF-8 é comum em Linux/macOS. Portuguese_Brazil é comum em Windows.
  try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
  except locale.Error:
    try:
      locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil')
    except locale.Error:
      # Se falhar, use a localidade padrão do sistema (pode não ser BR)
      # ou considere levantar um erro/logar um aviso.
      # Para este caso, vamos tentar usar o padrão para não quebrar.
      locale.setlocale(locale.LC_ALL, '') 
      print("Atenção: Não foi possível definir a localidade pt_BR. A formatação pode não ser a esperada.")

  if isinstance(value, int):
    # Formata como inteiro, com separadores de milhar
    return locale.format_string("%d", value, grouping=True)
  elif isinstance(value, float):
    # Formata como float, com 2 casas decimais e separadores de milhar
    # É importante notar que format_string pode ter comportamento diferente para floats
    # em versões mais antigas ou certas configurações.
    # Uma alternativa robusta para float é combinar o separador de milhar com replace da vírgula.
    # No entanto, locale.format_string com '%f' deve funcionar se o locale estiver correto.
    return locale.format_string("%.2f", value, grouping=True)
  else:
    raise TypeError("O valor deve ser um número (int ou float).")

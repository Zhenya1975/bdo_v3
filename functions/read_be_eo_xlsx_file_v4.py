import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB
from initial_values.initial_values import be_data_columns_to_master_columns, year_dict
from datetime import datetime
from initial_values.initial_values import sap_user_status_cons_status_list, be_data_cons_status_list, sap_system_status_ban_list
import sqlite3

db = extensions.db

date_time_plug = '1.1.2199'
date_time_plug = datetime.strptime(date_time_plug, '%d.%m.%Y')

# наименование итераций
iterations = {
  "Дата вывода 0 - итерация продления": "iteration_0",
  "Дата вывода 1 - итерация продления": "iteration_1",
  "Дата вывода 1 - итерация продления (корр 23.06.22)": "iteration_1_1",
  "Дата вывода 2 - итерация продления (корр 30.06.22)": "iteration_2"
}
iterations_list = ["operation_finish_date_iteration_0",	"operation_finish_date_iteration_1",	"operation_finish_date_iteration_2"]

def read_be_2_eo_xlsx():
  # читаем загруженный файл 
  be_eo_raw_data = pd.read_excel('uploads/be_eo_2_data.xlsx', sheet_name='be_eo_data', index_col = False)
  be_eo_data = be_eo_raw_data.rename(columns=be_data_columns_to_master_columns)

  be_eo_data['eo_code'] = be_eo_data['eo_code'].astype(str)
  be_eo_data['operation_finish_date_iteration_0'] = pd.to_datetime(be_eo_data['operation_finish_date_iteration_0'], format='%d.%m.%Y')
  be_eo_data['operation_finish_date_iteration_1'] = pd.to_datetime(be_eo_data['operation_finish_date_iteration_1'], format='%d.%m.%Y')
  be_eo_data['operation_finish_date_iteration_2'] = pd.to_datetime(be_eo_data['operation_finish_date_iteration_2'], format='%d.%m.%Y')
  

  # получаем данные из мастер-файла
  con = sqlite3.connect("database/datab.db")
  cursor = con.cursor()
  # sql = "SELECT * FROM eo_DB JOIN be_DB"
  sql = "SELECT \
  eo_DB.eo_code, \
  eo_DB.be_code, \
  be_DB.be_description, \
  eo_DB.eo_class_code, \
  eo_class_DB.eo_class_description, \
  models_DB.eo_model_name, \
  models_DB.eo_category_spec, \
  eo_DB.eo_model_id, \
  eo_DB.eo_description, \
  eo_DB.operation_start_date, \
  eo_DB.expected_operation_period_years, \
  eo_DB.sap_planned_finish_operation_date, \
  eo_DB.sap_system_status, \
  eo_DB.sap_user_status \
  FROM eo_DB \
  LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
  LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
  LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
  LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code"
  
  master_eo_df = pd.read_sql_query(sql, con)
  master_eo_df['operation_start_date'] = pd.to_datetime(master_eo_df['operation_start_date'])
  master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(master_eo_df['sap_planned_finish_operation_date'])

  # джойним данные из файла с мастер-данными
  be_master_data = pd.merge(be_eo_data, master_eo_df, on='eo_code', how='left')
  # be_master_data.to_csv('temp_data/be_master_data.csv')

  result_data_list = []
  # итерируемся по годам
  for year, year_data in year_dict.items():
    # print(year)
    year_first_date = datetime.strptime(year_data['period_start'], '%d.%m.%Y')
    year_last_date = datetime.strptime(year_data['period_end'], '%d.%m.%Y')

    # итерируемся по списку итераций
    for iteration in iterations_list:
      # итерируемся по списку ео в загруженном файле
      for row in be_master_data.itertuples():
        eo_code = getattr(row, 'eo_code')
        be_code = getattr(row, 'be_code')
        be_description = getattr(row, 'be_description')
        eo_class_code = getattr(row, 'eo_class_code')
        eo_class_description = getattr(row, 'eo_class_description')
        eo_model_name = getattr(row, 'eo_model_name')
        eo_model_id = getattr(row, 'eo_model_id')
        eo_category_spec = getattr(row, 'eo_category_spec')
        eo_description = getattr(row, "eo_description")
        operation_start_date = getattr(row, 'operation_start_date')
        operation_finish_date = getattr(row, iteration)
        iteration_name = iteration
        temp_dict = {}
        temp_dict['eo_code'] = eo_code
        temp_dict['be_code'] = be_code
        temp_dict['be_description'] = be_description
        temp_dict['eo_class_code'] = eo_class_code
        temp_dict['eo_class_description'] = eo_class_description
        temp_dict['eo_model_id'] = eo_model_id
        temp_dict['eo_model_name'] = eo_model_name
        temp_dict['eo_category_spec'] = eo_category_spec
        temp_dict['eo_description'] = eo_description
        temp_dict['operation_start_date'] = operation_start_date
        temp_dict['operation_finish_date'] = operation_finish_date
        temp_dict['iteration_name'] = iteration_name
        temp_dict['year'] = year
        result_data_list.append(temp_dict)
        
  iterations_df = pd.DataFrame(result_data_list) 
  iterations_df.to_csv('temp_data/iterations_df.csv')
        
        
        
        
        
        
import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB
from initial_values.initial_values import be_data_columns_to_master_columns, year_dict
from datetime import datetime
from initial_values.initial_values import sap_user_status_cons_status_list, be_data_cons_status_list, sap_system_status_ban_list, operaton_status_translation
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
  eo_DB.head_type, \
  be_DB.be_description, \
  eo_DB.eo_class_code, \
  eo_class_DB.eo_class_description, \
  models_DB.eo_model_name, \
  models_DB.eo_category_spec, \
  eo_DB.eo_model_id, \
  eo_DB.eo_description, \
  eo_DB.operation_start_date, \
  eo_DB.expected_operation_period_years, \
  eo_DB.operation_finish_date_calc, \
  eo_DB.sap_planned_finish_operation_date, \
  eo_DB.operation_finish_date_sap_upd, \
  eo_DB.sap_system_status, \
  eo_DB.sap_user_status \
  FROM eo_DB \
  LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
  LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
  LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
  LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code"
  
  master_eo_df = pd.read_sql_query(sql, con)
  master_eo_df = master_eo_df.loc[master_eo_df['head_type']=='head']
  master_eo_df['operation_start_date'] = pd.to_datetime(master_eo_df['operation_start_date'])
  master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(master_eo_df['sap_planned_finish_operation_date'])
  master_eo_df['operation_finish_date_sap_upd'] = pd.to_datetime(master_eo_df['operation_finish_date_sap_upd'])
  # джойним данные из файла с мастер-данными
  be_master_data = pd.merge(master_eo_df, be_eo_data, on='eo_code', how='left')
  be_master_data.to_csv('temp_data/be_master_data.csv')

  result_data_list = []
  # итерируемся по годам
  year_dict = {2022:{'period_start':'01.01.2022', 'period_end':'31.12.2022'}}
  iterations_list = ["operation_finish_date_iteration_0"]
  
  for year, year_data in year_dict.items():
    # print(year)
    year_first_date = datetime.strptime(year_data['period_start'], '%d.%m.%Y')
    year_last_date = datetime.strptime(year_data['period_end'], '%d.%m.%Y')

    # итерируемся по списку итераций
    for iteration in iterations_list:
      be_master_data[iteration].fillna(date_time_plug, inplace = True)
      be_master_data['operation_finish_date'] = be_master_data['operation_finish_date_sap_upd']
      be_master_data_temp = be_master_data.loc[be_master_data[iteration]!=date_time_plug]
      # be_master_data_temp.to_csv('temp_data/be_master_data_temp.csv')
      be_master_data_temp = be_master_data_temp.copy()
      be_master_data_temp[iteration] = pd.to_datetime(be_master_data_temp[iteration])

      indexes = list(be_master_data_temp.index.values)

      be_master_data = be_master_data.copy()
      be_master_data.loc[indexes, ['operation_finish_date']] = pd.to_datetime(be_master_data_temp[iteration])
      be_master_data.to_csv('temp_data/be_master_data_delete.csv')
      
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
        operation_status_rus = getattr(row, "operation_status")
        operation_status = 'in_operation'
        try:
          operation_status = operaton_status_translation[operation_status_rus]
        except:
          pass
        sap_user_status = getattr(row, "sap_user_status")
        sap_system_status = getattr(row, "sap_system_status")

        operation_start_date = getattr(row, 'operation_start_date')
        expected_operation_period_years = getattr(row, 'expected_operation_period_years')
        operation_finish_date_calc = getattr(row, 'operation_finish_date_calc')
        sap_planned_finish_operation_date = getattr(row, 'sap_planned_finish_operation_date')
        operation_finish_date_sap_upd = getattr(row, 'operation_finish_date_sap_upd')
        operation_finish_date_update_iteration = getattr(row, iteration)
        operation_finish_date = getattr(row, 'operation_finish_date')
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
        temp_dict['operation_status_rus'] = operation_status_rus
        temp_dict['sap_user_status'] = sap_user_status
        temp_dict['sap_system_status'] = sap_system_status
        temp_dict['operation_start_date'] = operation_start_date
        temp_dict['expected_operation_period_years'] = expected_operation_period_years
        temp_dict['operation_finish_date_calc'] = operation_finish_date_calc
        temp_dict['sap_planned_finish_operation_date'] = sap_planned_finish_operation_date
        temp_dict['operation_finish_date_sap_upd'] = operation_finish_date_sap_upd
        
        temp_dict['operation_finish_date_update_iteration'] = operation_finish_date_update_iteration
        temp_dict['operation_finish_date'] = operation_finish_date
        
        temp_dict['iteration_name'] = iteration_name
        temp_dict['year'] = year
        
        # определяем статус Эксплуатация
        #  если в пользовательском сап статусе нет статуса консервация
        # если в статусе загруженного файла нет слова Консервация
        # если дата начала эксплуатации меньше или равно последнего дня года
        # если дата завершения эксплуатации больше или равно первому дню года
        if sap_user_status not in sap_user_status_cons_status_list and \
        sap_system_status not in sap_system_status_ban_list and \
        operation_status != 'in_conservation' and \
        operation_start_date <= year_last_date and \
        operation_finish_date >= year_first_date:
          temp_dict['in_operation'] = 1
        else:
          temp_dict['in_operation'] = 0
        
        # определяем статус Ввод нового
        # если в пользовательском сап статусе нет статуса консервация
        # если в статусе загруженного файла нет слова Консервация  
        if sap_user_status not in sap_user_status_cons_status_list and \
        sap_system_status not in sap_system_status_ban_list and \
        operation_status != 'in_conservation' and \
        operation_start_date >= year_first_date and \
        operation_start_date <= year_last_date:
          temp_dict['add_new'] = 1
        else:
          temp_dict['add_new'] = 0
          
        result_data_list.append(temp_dict)
        
  iterations_df = pd.DataFrame(result_data_list) 
  iterations_df.to_csv('temp_data/iterations_df.csv')
        
        
        
        
        
        
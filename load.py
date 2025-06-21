import os
import pandas as pd
from extra.utils.extract import get_folder_input
import shutil


# Metodo para crear el folder data/output con particion
def create_folder_output_partition(partition:str):
    """
    Almacenar en las particiones del folder data/output
    """
    # Obtener el directorio padre
    dir_project = os.getcwd()

    # Directorio data output
    dir_data_output = os.path.join(dir_project, 'data/output')

    # Concatenar ruta destino con particion
    folder_output = os.path.join(dir_data_output, f'{partition[4:8]}/{partition[0:2]}/{partition[2:4]}')

    # Si el folder no existe, crearlo
    if not os.path.exists(folder_output):
        os.makedirs(folder_output)

    return folder_output


def save_df_output(df:pd.DataFrame, name_file:str, partition:str, ext:str):

    # Directorio base data output
    base_path_output = create_folder_output_partition(partition)

    if (ext == '.xlsx'):
        # Archivo procesado xlsx (name + dir)
        name_xlsx_file_output = f'{name_file}_{partition}.xlsx'
        dir_xlsx_file_output = os.path.join(base_path_output, name_xlsx_file_output)
        df.to_excel(dir_xlsx_file_output)

    elif (ext == '.csv'):
        # Archivo procesado csv (name + dir)
        name_xlsx_file_output = f'{name_file}_{partition}.csv'
        dir_xlsx_file_output = os.path.join(base_path_output, name_xlsx_file_output)
        df.to_csv(dir_xlsx_file_output)

def move_file_folder(name_file:str, partition:str):
    path_input_file = os.path.join(get_folder_input(), name_file)
    path_output_file = os.path.join(create_folder_output_partition(partition), name_file)

    # Mover el archivo original
    shutil.move(path_input_file, path_output_file)

def write_xlsx_masivo_vulneracion(file_name:str, partition:str, df_diff_final:pd.DataFrame, df_masivo_final:pd.DataFrame):
    file_name_final = f"{file_name}_{partition}.xlsx"
    path_output_file = os.path.join(create_folder_output_partition(partition), file_name_final)

    # Escribir los DataFrames en diferentes hojas
    with pd.ExcelWriter(path_output_file, engine='openpyxl') as writer:

        df_diff_final.to_excel(writer, sheet_name='Consolidado',index=False)
        df_masivo_final.to_excel(writer, sheet_name='Masivo', index=False)

    print(f'DataFrames guardados en {path_output_file}')





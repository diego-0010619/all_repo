import os
import re
import pandas as pd
import awswrangler as wr

def read_input_xlsx_regex(regex:str):
    # 1. Listar archivos .xlsx que hay en el folder input/ y leer el df (Solo 1 archivo)

    dir_path_input = get_folder_input()
    # Patron regex para validar nombre del archivo
    regex_partition = r'^Masivo_vulneracion_(\d{8})_.*.xlsx$'  # Este patrón asume un formato específico, ajusta según tus necesidades

    # Lista para almacenar los nombres de archivos Excel en el directorio
    archivos_excel = []

    # Iterar sobre los archivos en el directorio
    for archivo in os.listdir(dir_path_input):
        if archivo.endswith('.xlsx'):  # Puedes ajustar esto según el formato de tus archivos Excel
            archivos_excel.append(archivo)

    # Verificar la cantidad de archivos Excel
    cantidad_archivos = len(archivos_excel)

    if cantidad_archivos == 0:
        print("No se encontraron archivos Excel en el directorio.")
    elif cantidad_archivos == 1:
        #   Leer el archivo Excel
        # Validar el nombre del archivo con la expresión regular
        if re.match(regex_partition, archivos_excel[0]):

            file_input_path = os.path.join(dir_path_input, archivos_excel[0])

            # Asignar valor de input file
            df_input_file = pd.read_excel(file_input_path)
            df_input_file = df_input_file[['ID del ticket']]

            # Asignar valor de original del file
            name_input_file = archivos_excel[0]

            """
            Extraer particion del nombre del archivo original (insumo)
            """

                    # Aplicar la expresión regular al nombre ORIGINAL del archivo
            match = re.search(regex_partition, archivos_excel[0])

            # Verificar si se encontró el grupo de captura (particion)
            if match:
                partition = match.group(1)
            else:
                print("No se encontró el número en el nombre del archivo.")

            print(f"File nombrado OK, shape df_input_file: {df_input_file.shape}")
            return df_input_file, partition, name_input_file
        else:
            print("Archivo mal nombrado")
            return None
    else:
        return None
        print("Hay más de un archivo")



def execute_query(query:str, database_athena:str, workgroup_athena:str):
    try:
        df_query = wr.athena.read_sql_query(sql=query,
                                            database=database_athena,
                                            workgroup=workgroup_athena,
                                            ctas_approach = False)
        return df_query
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def get_folder_input():

    # Obtener el directorio padre
    dir_project = os.getcwd()
    # Directorio input data de insumo
    dir_path_input = os.path.join(dir_project, 'data/input')

    return dir_path_input
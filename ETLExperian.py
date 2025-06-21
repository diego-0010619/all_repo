import pandas as pd
import numpy as np

class ETLExperian:

    def __init__(self) -> None:
        self._tf_factory = TransformFactory()
        pass
    
    def generate_df_final_eliminacion_experian(self, df_remaing:pd.DataFrame, df_trx:pd.DataFrame):

        df_curated = df_trx.copy()
        df_remaing = df_remaing.copy()
        # 1. Lecturas

        # 2. Transfomaciones
        df_good, df_wrong = self._tf_factory._tfs_df_masivos_vulneraciones(df_remaing=df_remaing,
                                                                           df_trx=df_curated)
        # 3. Cargas de datos

        return df_good, df_wrong


class TransformFactory:

    def __init__(self) -> None:
        self._tf_columns = TransformColumns()
        self._tf_records = TransformRecords()
        pass

    def _tfs_df_masivos_vulneraciones(self, df_remaing:pd.DataFrame, df_trx:pd.DataFrame):
      
        # 1. Asegurar solo transacciones que cruzaron completamente
        df_complete_finacle, df_wrong_finacle = self._tf_records._drop_rows_no_finacle(df=df_trx)

        # 2. Agg codigo_masivo df con datos FINACLE
        df_finacle_cod_masivo = self._tf_columns._agg_column_codigo_masivo_finacle(df=df_complete_finacle)

        df_numero_producto = self._tf_columns._merge_num_producto(df_remaing=df_remaing,
                                                                  df=df_finacle_cod_masivo)

        # 3. Realizar group by para total de monto por ticket y max fecha de trx Finacle
        df_total_monto_finacle = self._tf_records._group_by_total_monto_finacle(df=df_numero_producto)
      
        # 4. Añadir codigo_masivo df con datos REMAING
        df_remaing_cod_masivo = self._tf_columns._agg_column_codigo_masivo_zendesk(df=df_remaing)

        # 5. Realizar merge final
        df_merge_final = self._tf_columns._merge_data_zendesk(df_remaing=df_remaing_cod_masivo,
                                                        df_final_montos=df_total_monto_finacle)

        # 6. Columna de diff y rename columns FINAL
        df_final = self._tf_columns._agg_column_diff(df=df_merge_final)
        # df_final = self._tf_columns._cast_columns(df=df_drop_columns)

        return df_final, df_wrong_finacle 



class TransformColumns:

    # Metodo para eliminar espacios en los nombres de las columnas
    def _strip_name_columns_df(self, df: pd.DataFrame):
        df.columns = list(map(lambda x: x.strip(), df.columns))

    # Metodo para eliminar espacios blanco columnas tipo (object,str)
    def _strip_columns_str(self, df: pd.DataFrame):
        # Seleccionar solo las columnas de tipo object (str) o string
        df_columns_str = df.select_dtypes(include=['object', 'string'])

        # Obtener los nombres de las columnas seleccionadas
        name_columns_str = df_columns_str.columns.tolist()

        # Aplicar strip a cada columna
        for i in name_columns_str:
            df[i] = df[i].apply(lambda x: x.strip())
    
    # Metodo para agg id_visa a solo P017
    def _agg_column_id_visa(self, df: pd.DataFrame):
        condition_concepto = (df['codigo_concepto'].astype(str).str.strip() == 'P017')
        df.loc[condition_concepto, 'id_visa'] = df['trans_particular_original_add'].astype(str).str.slice(0,15)
        return df
    
    # Metodo para agg columna 'codigo_masivo' DATOS FINACLE
    def _agg_column_codigo_masivo_finacle(self, df:pd.DataFrame):
  
        # Lista codigos_concepto impuestos
        list_impuestos = ['I001','I002', 'I003']
        # Condicion
        condition = (df['codigo_concepto'].isin(list_impuestos))
        # Caso a que no son impuesto a -> A017
        df.loc[condition, 'codigo_masivo'] = 'I003'
        # Caso a que no son impuesto a -> A017
        df.loc[~condition, 'codigo_masivo'] = 'A017'

        # df['codigo_masivo'] = df['codigo_concepto'].apply(lambda x: 'I001' if x == 'I001' else ('I002' if x == 'I002' else ('I003' if x == 'I003' else 'A017')))

        return df
    
    # Metodo para agg columna 'codigo_masivo' DATOS ZENDESK (REMAING)
    def _agg_column_codigo_masivo_zendesk(self, df:pd.DataFrame):
        
        # Agg columna codigo masivo
        df['codigo_masivo'] = 'A017'

        return df
    
    # Metodo para realizar merge por ticket_id para complemetar submodalidad y monto_zendesk
    def _merge_data_zendesk(self, df_remaing:pd.DataFrame, df_final_montos:pd.DataFrame):
        
        """Asegurar tipo de datos de df_remaing"""
        # Reemplazar valores en blanco por NaN
        df_remaing.replace('', np.nan, inplace=True)
        # Cast tipo Float, valor del fraude
        df_remaing['valor_del_fraude'] = df_remaing['valor_del_fraude'].astype('Float64')

        
        df_aux_monto_zendesk = df_remaing[['ticket_id','codigo_masivo','valor_del_fraude']]
        df_aux_submodalidad = df_remaing[['ticket_id', 'submodalidad_fraude']]
        
        
        
        # df con solo valor de zendesk
        """ Realizar los merge """
        df_final_montos = pd.merge(   left=df_final_montos,
                              right=df_aux_monto_zendesk,
                              left_on=['ticket_id','codigo_masivo'],
                              right_on=['ticket_id','codigo_masivo'],
                              how='left'
                    )
        
        # df con solo submodalidades
        df_final_submodalidad = pd.merge(left=df_final_montos,
                                         right=df_aux_submodalidad,
                                         left_on=['ticket_id'],
                                         right_on=['ticket_id'],
                                         how='left'
                    )
        
        return df_final_submodalidad
    

    # Metodo para realizar merge por ticket_id para complemetar submodalidad y monto_zendesk
    def _merge_num_producto(self, df_remaing:pd.DataFrame, df:pd.DataFrame):

        df_aux_num_prod_zendesk = df_remaing[['ticket_id','numero_producto']]
        
        # df con solo valor de zendesk
        """ Realizar los merge """
        df_num_producto = pd.merge(   left=df,
                              right=df_aux_num_prod_zendesk,
                              left_on=['ticket_id'],
                              right_on=['ticket_id'],
                              how='left'
                    )

        return df_num_producto

    
    def _agg_column_diff(self, df:pd.DataFrame):

        # Rename Columns
        columns_name = {
            'valor_transaccion': 'monto_finacle',
            'valor_del_fraude': 'monto_zendesk'
        }
        df = df.rename(columns=columns_name)

        # Rellenar con 0 los faltantes (impuestos monto_zendesk)
        df['monto_zendesk'] = df['monto_zendesk'].fillna(0)

        # Realizar la diferencia
        df['diff'] = df['monto_finacle'] - df['monto_zendesk']

        return df


class TransformRecords:


    # Metodo para asegurar suma de todas las transaccion cruzaron con Finacle
    def _drop_rows_no_finacle(self, df:pd.DataFrame):

        # Traer todos los tickets_id que cruzaron con finacle (tienen valor_transacción)
        tickets_invalid = df.loc[df["valor_transaccion"].isnull(), "ticket_id"]
        # En df_finacle solo dejar los COMPLETOS
        df_finacle = df.query('ticket_id not in @tickets_invalid')
        # Si una transaccion salio mal, dejar TODO lo demas en el df_finacle_blk_list 
        df_finacle_blk_list = df.query('ticket_id in @tickets_invalid')

        return df_finacle, df_finacle_blk_list
    
    # Metodo que agrupa la suma total trx por cada ticket
    def _group_by_total_monto_finacle(self, df:pd.DataFrame):

        # Asegurar tipo Float64 en valor_transaccion
        df['valor_transaccion'] = df['valor_transaccion'].astype('Float64')
        # Totalizar por usuario y código
        df_total_masivo_ticket = df.groupby(['ticket_id','numero_producto', 'codigo_masivo'])\
                                    .agg({
                                            'fecha_posteo': 'max',
                                            'valor_transaccion': 'sum'
                                        }).reset_index()
 
        return df_total_masivo_ticket



   
import pandas as pd
from extra import constants
import numpy as np


# Metodo para retornar df con trx complete, incomplete
def trx_finacle_complete_incomplete(df: pd.DataFrame):
    df = df.copy()

    # Quitar los impuestos NO FRAUDES I001, I002
    df = df.loc[~df['codigo_concepto'].isin(constants.list_impuestos_no_fraude)]

    # Traer todos los tickets_id que cruzaron con finacle (tienen valor_transacción)
    tickets_invalid = df.loc[df["valor_transaccion"].isnull(), "ticket_id"]
    # En df_finacle solo dejar los COMPLETOS
    df_finacle_complete = df.query('ticket_id not in @tickets_invalid')
    # Si una transaccion salio mal, dejar TODO lo demas en el df_finacle_blk_list
    df_finacle_incomplete = df.query('ticket_id in @tickets_invalid')

    return df_finacle_complete, df_finacle_incomplete

def agg_sum_id_pago_complete(df_complete: pd.DataFrame, df_remaining: pd.DataFrame):
    # Crear copia Dataframes
    df_complete = df_complete.copy()
    df_remaining = df_remaining.copy()

    # Totalizar por usuario y código Valor Finacle
    df_total_ticket = df_complete.groupby(['ticket_id']) \
        .agg(
        num_tran_id=('num_tran_id', 'max'),
        valor_finacle=('valor_transaccion', lambda x: x[df_complete['codigo_concepto'] != 'I003'].sum()),
        valor_gmf=('valor_transaccion', lambda x: x[df_complete['codigo_concepto'] == 'I003'].sum())
    ).reset_index()

    # Asegurar tipo de dato 'submodalidad_fraude'
    df_remaining['submodalidad_fraude'] = df_remaining['submodalidad_fraude'].astype(str)

    # Convertir cadenas vacias a np.nan
    condition = (df_remaining['submodalidad_fraude'].str.strip() == '')
    df_remaining.loc[condition,'submodalidad_fraude'] = np.nan

    # Adjuntar valor total de Zendesk (df_remaining)
    df_total_finacle_zendesk = pd.merge(left=df_total_ticket,
                                        right=df_remaining[['ticket_id', 'valor_del_fraude', 'submodalidad_fraude']],
                                        on='ticket_id',
                                        how='left')\
                                    .rename(columns={'valor_del_fraude':'valor_zendesk'
                                                     })

    # Realizar diferencia entre valor_finacle - valor_zendesk
    df_total_finacle_zendesk['valor_zendesk'] = df_total_finacle_zendesk['valor_zendesk'].astype('Float64').fillna(0)
    df_total_finacle_zendesk['diff'] = df_total_finacle_zendesk['valor_finacle'] - df_total_finacle_zendesk['valor_zendesk']

    # Agregar columna 'id_pago'
    df_total_id_pago = agg_column_id_pago(df=df_total_finacle_zendesk)

    # Agregar columna 'comentario'
    df_total_id_pago.loc[:,'comentario'] = 'Ok'

    return df_total_id_pago

def agg_sum_id_pago_incomplete(df_incomplete: pd.DataFrame, df_remaining: pd.DataFrame):
    # Crear copia Dataframes
    df_incomplete = df_incomplete.copy()
    df_remaining = df_remaining.copy()

    # Generar lista tran_id que no cruzaron
    df_blk_trx = df_incomplete.loc[df_incomplete['valor_transaccion'].isna()] \
        .groupby('ticket_id')['tran_id'].apply(lambda x: ', '.join(x)).reset_index() \
        .rename(columns={'tran_id':'comentario'})
    # Agg columna de comentario
    df_blk_trx['comentario'] =  df_blk_trx['comentario'].apply(lambda x: 'No cruzan Tran IDs: ' + str(x))

    # Totalizar por usuario y código Valor Finacle
    df_total_ticket = df_incomplete.groupby(['ticket_id']) \
        .agg({
        'num_tran_id': 'max',
    }).reset_index()

    df_total_ticket.loc[:,'valor_transaccion'] = np.nan
    # Asegurar tipo de dato 'submodalidad_fraude'
    df_remaining['submodalidad_fraude'] = df_remaining['submodalidad_fraude'].astype(str)

    # Convertir cadenas vacias a np.nan
    condition = (df_remaining['submodalidad_fraude'].str.strip() == '')
    df_remaining.loc[condition,'submodalidad_fraude'] = np.nan

    # Adjuntar valor total de Zendesk (df_remaining)
    df_total_finacle_zendesk = pd.merge(left=df_total_ticket,
                                        right=df_remaining[['ticket_id', 'valor_del_fraude', 'submodalidad_fraude']],
                                        on='ticket_id',
                                        how='left') \
        .rename(columns={'valor_del_fraude':'valor_zendesk',
                         'valor_transaccion':'valor_finacle'})

    # Realizar diferencia entre valor_finacle - valor_zendesk
    df_total_finacle_zendesk['valor_zendesk'] = df_total_finacle_zendesk['valor_zendesk'].astype('Float64').fillna(0)
    df_total_finacle_zendesk.loc[:,'diff'] = np.nan

    # Agregar id_pago
    df_total_finacle_zendesk.loc[:,'id_pago'] = np.nan

    # Agregar valor_gmf
    df_total_finacle_zendesk.loc[:,'valor_gmf'] = np.nan

    df_diff_incomplete_final = pd.merge(left=df_total_finacle_zendesk,
                                        right=df_blk_trx,
                                        on=['ticket_id'],
                                        how='left')

    return df_diff_incomplete_final

# Funcion deprecated
def agg_column_id_pago(df: pd.DataFrame):
    df = df.copy()

    """
    Condiciones para pago
    """

    # Condicion monto inferior
    condition_monto_inferior = ((df['valor_finacle'] >= 0.0) & (df['valor_finacle'] <= float(constants.limite_monto_inferior)))
    # Condicion monto superior
    condition_monto_superior = (~condition_monto_inferior)

    # Aplicar regla monto_inferior
    df.loc[condition_monto_inferior,'id_pago'] = 'monto_inferior'
    # Aplicar regla monto_superior (depende de la submodalidad)
    df.loc[condition_monto_superior, 'id_pago'] = df['submodalidad_fraude']

    return df

# Metodo para agregar id_pago por transaccion
def agg_column_id_pago_transaccion(df: pd.DataFrame, df_id_pago: pd.DataFrame):
    df = df.copy()
    df_id_pago = df_id_pago.copy()

    # Quitar los impuestos NO FRAUDES I001, I002
    df = df.loc[~df['codigo_concepto'].isin(constants.list_impuestos_no_fraude)]

    # Adjuntar id_pago por trx donde la suma sea completa (tickets donde cada trx cruzó)
    df_trx_id_pago = pd.merge(  left=df,
                                right=df_id_pago[['ticket_id', 'id_pago']],
                                on='ticket_id',
                                how='left')

    # Modificar id_pago a las transacciones con impuestos
    df_trx_id_pago = df_trx_id_pago.copy()

    # Condicion impuestos
    condition_impuestos = (df_trx_id_pago['codigo_concepto'].isin(constants.list_impuestos))
    # Aplicar regla impuestos
    df_trx_id_pago.loc[condition_impuestos,'id_pago'] = "impuesto_" + df_trx_id_pago['codigo_concepto']

    return df_trx_id_pago

# Metodo para agregar columna 'id_pago'
def agg_column_id_pago_2(df: pd.DataFrame, df_remaining: pd.DataFrame):

    df = df.copy()
    df_remaining = df_remaining.copy()

    # Join transacciones con submodalidades
    df_join = join_submodalidad_fraude(df=df,
                                       df_remaining=df_remaining)

    # Lista de impuestos
    list_impuestos = ['I001','I002', 'I003']

    """
    Condiciones para pago
    """

    # Condicion monto inferior
    condition_monto_inferior = ((df_join['valor_transaccion'] >= 0.0) & (df_join['valor_transaccion'] <= float(constants.limite_monto_inferior))) & (~df_join['codigo_concepto'].isin(list_impuestos))
    # Condicion impuestos
    condition_impuestos = (df_join['codigo_concepto'].isin(list_impuestos))
    # Condicion monto superior
    condition_monto_superior = (~condition_monto_inferior) & (~condition_impuestos)

    # Aplicar regla monto_inferior
    df_join.loc[condition_monto_inferior,'id_pago'] = 'monto_inferior'
    # Aplicar regla impuestos
    df_join.loc[condition_impuestos,'id_pago'] = "impuesto_" + df['codigo_concepto']
    # Aplicar regla monto_superior (depende de la submodalidad)
    df_join.loc[condition_monto_superior, 'id_pago'] = df_join['submodalidad_fraude']

    return df_join


def join_submodalidad_fraude(df: pd.DataFrame, df_remaining: pd.DataFrame):
    df = df.copy()
    df_remaining = df_remaining.copy()

    # Asegurar tipo de dato 'submodalidad_fraude'
    df_remaining['submodalidad_fraude'] = df_remaining['submodalidad_fraude'].astype(str)

    # Convertir cadenas vacias a np.nan
    condition = (df_remaining['submodalidad_fraude'].str.strip() == '')
    df_remaining.loc[condition,'submodalidad_fraude'] = np.nan

    # Adjuntar valor total de Zendesk (df_remaining)
    df_trx_submodalidad_fraude = pd.merge(left=df,
                                        right=df_remaining[['ticket_id', 'submodalidad_fraude']],
                                        on='ticket_id',
                                        how='left')

    return df_trx_submodalidad_fraude

# Metodo final para construir masivo
def build_masivo_vulneracion(df_input_file: pd.DataFrame, df: pd.DataFrame):
    df_input_file = df_input_file.copy()
    df = df.copy()

    # Rename input_file ticket_id
    df_input_file = df_input_file.rename(columns={'ID del ticket':'ticket_id'})

    df_data_id_pago = pd.DataFrame(constants.data_id_pago)
    df_final = pd.merge(left=df,
                        right=df_data_id_pago,
                        on='id_pago',
                        how='left')
    condition_trans_particular_credito = (df_final['flag_date_trans_particular_credito'].notnull()) & (df_final['flag_date_trans_particular_credito'] == True)

    condition_trans_particular_debito = (df_final['flag_date_trans_particular_debito'].notnull()) & (df_final['flag_date_trans_particular_debito'] == True)

    condition_trans_particular_2 = (df_final['flag_number_trans_particular_2'].notnull()) & (df_final['flag_number_trans_particular_2'] == True)

    df_masivo = pd.DataFrame(None)

    # Generar df masivo con reglas equipo operaciones
    df_masivo = df_masivo.assign(
        ticket_id = df_final['ticket_id'],
        numero_de_cuenta_a_debitar =  df_final['numero_de_cuenta_a_debitar'],
        numero_de_cuenta_a_acreditar = df_final['numero_producto'],
        monto = df_final['valor_transaccion'],
        trans_particular_debito = np.where(condition_trans_particular_debito,
                                           df_final['prefix_trans_particular_debito'] + df_final["fecha_posteo"].dt.strftime('%d%m%Y'), df_final['prefix_trans_particular_debito']),
        trans_particular_credito = np.where(condition_trans_particular_credito,
                                            df_final['prefix_trans_particular_credito'] + df_final["fecha_posteo"].dt.strftime('%d%m%Y'), df_final['prefix_trans_particular_credito']),
        tran_code = df_final['tran_code'],
        trans_particular_2 = np.where(condition_trans_particular_2,
                                      df_final['prefix_trans_particular_2'] + df_final["ticket_id"].astype(str), df_final['prefix_trans_particular_2']),
        tran_remarks = df_final['tran_id'],
    ).sort_values(by=['ticket_id'], ascending=[True]).reset_index(drop=True)

    # Cruzar con IDs input
    df_masivo_final = pd.merge(left=df_input_file,
                              right=df_masivo,
                              on='ticket_id',
                              how='left') \
        .sort_values(by=['ticket_id'], ascending=[True]).reset_index(drop=True)

    return df_masivo_final

def build_diff_vulneracion(df_input_file: pd.DataFrame, df_diff_complete: pd.DataFrame, df_diff_incomplete: pd.DataFrame):
    df_input_file = df_input_file.copy()
    df_diff_complete = df_diff_complete.copy()
    df_diff_incomplete = df_diff_incomplete.copy()

    df_diff_final = pd.concat([df_diff_complete,df_diff_incomplete])

    # Rename column 'ticket_id'
    df_input_file = df_input_file.rename(columns={'ID del ticket':'ticket_id'})

    df_diff_concat = pd.merge(left=df_input_file,
                                right=df_diff_final,
                                on='ticket_id',
                                how='left')\
                        .sort_values(by=['ticket_id'], ascending=[True]).reset_index(drop=True)

    # Completar comentario a ticket, no encontrados
    df_diff_concat['comentario'] = df_diff_concat['comentario'].fillna('Ticket no encontrado, validar manualmente')

    return df_diff_concat


def highlight_df_diff(s):
    # Clave a extraer
    key_to_extract = 'id_pago'

    # Extraer los valores soportados
    list_id_pago = [d[key_to_extract] for d in data_id_pago if key_to_extract in d]

    if pd.notna(s["diff"]):
        # Con diferencia entre Zendesk y Finacle, pero Trx completas
        if (s["diff"] != 0 and s["comentario"] == "Ok"):
            return ['background-color: #FFCA33']*9
        # Mal tipicados en submodalidad con todo ok
        elif (s["diff"] == 0 and s["id_pago"] not in list_id_pago):
            return ['background-color: #D3CFC4']*9
    else:
        return ['background-color: #D9421F']*9


def highlight_df_masivo_final(s):
    # Marcar tickets que no cruzaron con Finacle ó Zendesk ó no soportados por mala tipificacion

    if pd.isna(s["numero_de_cuenta_a_debitar"]):
        return ['background-color: #D9421F']*9



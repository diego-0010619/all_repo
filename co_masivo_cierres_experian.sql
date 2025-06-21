-- Diccionario atributos curated del producto de datos
{% set columns = [
    {'column': 'ticket_id', 'data_type': 'BIGINT', 'alias': 'ticket_id'},
    {'column': 'created_at', 'data_type': 'TIMESTAMP', 'alias': 'fecha_creacion'},
    {'column': 'account_number', 'data_type': 'STRING', 'alias': 'numero_cuenta'},
    {'column': 'nombre_del_solicitante', 'data_type': 'STRING', 'alias': 'nombre_solicitante'},
    {'column': 'tipo_de_documento', 'data_type': 'STRING', 'alias': 'tipo_identificacion'},
    {'column': 'numero_de_documento', 'data_type': 'STRING', 'alias': 'numero_identificacion'},
    {'column': 'celular_de_quien_se_comunica', 'data_type': 'STRING', 'alias': 'celular_quien_comunica'},
    {'column': 'codigo_experian', 'data_type': 'STRING', 'alias': 'codigo_experian'},
    {'column': 'error_description', 'data_type': 'STRING', 'alias': 'error_description'},
    {'column': 'closure_notes', 'data_type': 'STRING', 'alias': 'closure_notes'},
    {'column': 'year', 'data_type': 'STRING', 'alias': 'year'},
    {'column': 'month', 'data_type': 'STRING', 'alias': 'month'},
    {'column': 'day', 'data_type': 'STRING', 'alias': 'day'},
] %}

{{
  config(
    table_type='hive',
    format='parquet',
    incremental_strategy='overwrite'
)
}}
-- materialized='incremental',
-- partitioned_by=['year','month','day']
-- Cambiar fuente de datos por ambiente

WITH

dp_cierres_experian AS (
    SELECT  ft.ticket_id
        ,   ft.created_at
        ,   apr.account_number
        ,   ft.fields_payload['nombre_del_solicitante'] AS nombre_del_solicitante
        ,   ft.fields_payload['tipo_de_documento'] AS tipo_de_documento
        ,   ft.fields_payload['numero_de_documento'] AS numero_de_documento
        ,   ft.fields_payload['celular_de_quien_se_comunica'] AS celular_de_quien_se_comunica
        ,   CASE
                 WHEN   (apr."closure_notes" = 'Suplantacion'
                        AND (apr."error_description" = 'Account Closed Successfully'
                         OR apr."error_description" = 'Account is Already Closed'))
                     THEN CONCAT(
                             RPAD(ft.fields_payload['numero_de_cuenta_870'], 18, '0'),
                             LPAD(ft.fields_payload['numero_de_documento'], 11, '0'),
                             (  CASE
                                  WHEN ft.fields_payload['tipo_de_documento'] = 'tarjeta_cédula_de_extranjería' THEN '4'
                                  WHEN ft.fields_payload['tipo_de_documento'] = 'tarjeta_tarjeta_de_identidad'  THEN '7'
                                  ELSE '1'
                                END),
                            'E')
                 ELSE NULL
            END AS codigo_experian
        ,   apr.error_description
        ,   apr.closure_notes
        ,   apr.year
        ,   apr.month
        ,   apr.day
    FROM    {{source('servicio_raw','co_zendesk_flatten_tickets')}} ft
    RIGHT JOIN  {{source('gestionfraude_raw','co_internal_carga_manual_apr_cierre')}} apr
            ON TRIM(ft.fields_payload['numero_de_cuenta_870']) = apr.account_number
    WHERE   TRUE
            AND ft.ticket_form_label = 'gestion_de_fraude'
            AND brand_id = 1088748
    ORDER BY created_at DESC
),

cast_curated_dp AS (
    SELECT  {{ cast_datatypes_athena(columns) }} -- Macro datatypes
    FROM    dp_cierres_experian
)

SELECT  *
FROM cast_curated_dp
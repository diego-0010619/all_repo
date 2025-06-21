{{
  config(
    table_type='iceberg',
    format='parquet',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['numero_producto','ticket_id']
)
}}

-- Diccionario de atributos del producto de datos
{% set columns = [
    {'column': 'nombre_completo', 'data_type': 'STRING', 'alias': 'nombre_completo'},
    {'column': 'numero_identificacion', 'data_type': 'STRING', 'alias': 'numero_identificacion'},
    {'column': 'valor_transaccion', 'data_type': 'DECIMAL(18, 4)', 'alias': 'valor_transaccion'},
    {'column': 'naturaleza_transaccion', 'data_type': 'STRING', 'alias': 'naturaleza_transaccion'},
    {'column': 'codigo_concepto', 'data_type': 'STRING', 'alias': 'codigo_concepto'},
    {'column': 'numero_de_cuenta_restitucion_dinero_fraude', 'data_type': 'STRING', 'alias': 'numero_de_cuenta_restitucion_dinero_fraude'},
    {'column': 'ticket_id', 'data_type': 'BIGINT', 'alias': 'ticket_id'},
    {'column': 'numero_producto', 'data_type': 'STRING', 'alias': 'numero_producto'},
    {'column': 'fecha_transaccion', 'data_type': 'DATE', 'alias': 'fecha_transaccion'},
    {'column': 'fecha_posteo', 'data_type': 'STRING', 'alias': 'fecha_posteo'},
    {'column': 'codigo_rastreo', 'data_type': 'STRING', 'alias': 'codigo_rastreo'},
    {'column': 'numero_cuenta_contable', 'data_type': 'STRING', 'alias': 'numero_cuenta_contable'},
    {'column': 'dias_transcurridos', 'data_type': 'BIGINT', 'alias': 'dias_transcurridos'},
    {'column': 'execution_date', 'data_type': 'DATE', 'alias': 'execution_date'},
] %}

WITH
co_zendesk_abonos_monto_inferior_fraude as (
    /*
        Grupo de datos que contiene la información de todos los tickets que están en estado 'hold', 'open' y 'pending'
        Los cuales son candidatos para abonar y deben ser analizados.
    */
    SELECT
        TRIM(fields_payload['estado_del_abono'])                                            as estado_del_abono,
        CASE WHEN TRIM(fields_payload['nombre_del_solicitante']) = ''
            THEN TRIM(requester_name)
                ELSE TRIM(fields_payload['nombre_del_solicitante']) END                     as nombre_del_solicitante,
        TRIM(fields_payload['tipo_de_documento'])                                           as tipo_de_documento,
        TRIM(fields_payload['numero_de_documento'])                                         as numero_de_documento,
        TRIM(fields_payload['valor_del_fraude'])                                            as valor_del_fraude,
        ticket_id                                                                           as ticket,
        TRIM(fields_payload['numero_de_cuenta_870'])                                        as numero_de_cuenta,
        TRIM(fields_payload['numero_de_cuenta_870_para_restituir_el_dinero_del_fraude'])    as numero_de_cuenta_restitucion_dinero_fraude,
        TRIM(fields_payload['genero'])                                                      as genero,
        CASE WHEN TRIM(submitter_role) = 'agent' THEN submitter_name ELSE '' END            as nombre_del_agente,
        TRIM(fields_payload['cantidad_de_transacciones_reportadas'])                        as cantidad_de_transacciones_reportadas,
        TRIM(fields_payload['productos_y_canales'])                                         as productos_y_canales,
        TRIM(fields_payload['modalidad_de_fraude'])                                         as modalidad_de_fraude,
        TRIM(fields_payload['submodalidad_del_fraude'])                                     as submodalidad_del_fraude,
        TRIM(fields_payload['biometria_consistente'])                                       as biometria_consistente,
        TRIM(fields_payload['celular_de_quien_se_comunica'])                                as numero_celular_de_quien_se_comunica,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_01'])           as tran_id_01,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_02'])           as tran_id_02,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_03'])           as tran_id_03,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_04'])           as tran_id_04,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_05'])           as tran_id_05,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_06'])           as tran_id_06,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_07'])           as tran_id_07,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_08'])           as tran_id_08,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_09'])           as tran_id_09,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_10'])           as tran_id_10,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_11'])           as tran_id_11,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_12'])           as tran_id_12,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_13'])           as tran_id_13,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_14'])           as tran_id_14,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_15'])           as tran_id_15,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_16'])           as tran_id_16,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_17'])           as tran_id_17,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_18'])           as tran_id_18,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_19'])           as tran_id_19,
        TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_20'])           as tran_id_20,
        TRIM(type)                                                                          as type,
        TRIM(fields_payload['tipo_de_solucion'])                                            as tipo_de_solucion,
        CASE WHEN TRIM(fields_payload['dictamen_del_fraude']) = 'descartado' THEN 'Favorable para el cliente con abono de dinero' ELSE TRIM(fields_payload['dictamen_del_fraude']) END as dictamen_del_fraude
    FROM {{source('servicio_raw','co_zendesk_flatten_tickets')}}
    WHERE   true
            AND ticket_form_label = 'gestion_de_fraude'
            AND brand_id = 1088748 --Colombia
            AND status IN ('hold', 'open', 'pending')
            AND type = 'incident'
            AND group_id IN (13850956415501)
            AND TRIM(fields_payload['modalidad_de_fraude']) IN ('vulneración_de_cuentas_modalidad', 'compras_no_reconocidas_con_tarjeta_débito_modalidad')
            AND TRIM(fields_payload['submodalidad_del_fraude']) IN ('transacción_no_reconocida_favorable_en_primer_contacto_gestión_fraude', 'telemercadeo_gestión_fraude')
),
base_trx AS (
    /*
        Grupo de datos que contiene el cruce de los datos entre la información de Zendesk y la tabla finacle transacción uso
        Con el fin de conocer la fecha del último abono generado por gestión de fraude por medio de los conceptos 'A017', 'A105', 'A079', 'A082' y 'A078'
    */
    SELECT
        CASE WHEN ft.otros_nombres = ' ' THEN ft.primer_nombre || ' ' || ft.primer_apellido || ' ' || ft.segundo_apellido
            ELSE ft.primer_nombre || ' ' || ft.otros_nombres || ' ' || ft.primer_apellido || ' ' || ft.segundo_apellido  END AS nombre_completo
        , ft.numero_identificacion                               AS numero_identificacion
        , ft.valor_transaccion                                   AS valor_transaccion
        , trim(ft.naturaleza_transaccion)                        AS naturaleza_transaccion
        , trim(ft.codigo_concepto)                               AS codigo_concepto
        , BZ.numero_de_cuenta_restitucion_dinero_fraude          AS numero_de_cuenta_restitucion_dinero_fraude
        , BZ.ticket                                              AS ticket_id
        , ft.numero_producto                                     AS numero_producto
        , DATE(ft.fecha_transaccion)                             AS fecha_transaccion
        , ft.fecha_posteo                                        AS fecha_posteo
        , CASE
              WHEN trim(ft.trans_particular_contraparte_add) = '' THEN NULL
              ELSE trim(ft.trans_particular_contraparte_add) END AS codigo_rastreo
        , trim(CASE
                   WHEN substring(ft.numero_cuenta_contraparte, 1, 2) = 'BD'
                       THEN trim(ft.numero_cuenta_contraparte)
                   ELSE '-1' END)                                AS numero_cuenta_contable
        , RANK() OVER (PARTITION BY ft.numero_identificacion ORDER BY ft.fecha_transaccion DESC) AS ranking
    FROM {{source('productos_raw','finacle_transaccional_uso')}} ft
    INNER JOIN co_zendesk_abonos_monto_inferior_fraude BZ on ft.numero_identificacion = BZ.numero_de_documento
    WHERE true
    AND CAST(ft.fecha_transaccion AS DATE) BETWEEN DATE_ADD('month', -12, DATE('{{ var("execution_date") }}')) AND DATE('{{ var("execution_date") }}')
    AND ft.codigo_concepto IN ('A017', 'A105', 'A079', 'A082', 'A078')
),
abonos_dobles AS (
    /*
        Grupo de datos con el resultado final de candidatos para abonos dobles. Después de cruzar entre Zendesk y Finacle.
    */
    SELECT
        *,
        DATE_DIFF('day', fecha_transaccion, DATE('{{ var("execution_date") }}')) AS dias_transcurridos,
        DATE('{{ var("execution_date") }}') AS execution_date
    FROM base_trx
    WHERE ranking = 1
    AND DATE_DIFF('day', fecha_transaccion, DATE('{{ var("execution_date") }}')) <= 365
)

SELECT  {{ cast_datatypes_athena(columns) }} FROM abonos_dobles
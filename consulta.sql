SELECT  
    ticket_id,
    created_at,
    fields_payload['numero_de_cuenta_870'] as numero_de_cuenta_870,
    fields_payload['nombre_del_solicitante'] as nombre_del_solicitante,
    fields_payload['tipo_de_documento'] as tipo_documento,
    fields_payload['numero_de_documento'] as numero_de_documento,
    fields_payload['celular_de_quien_se_comunica'] as celular_de_quien_se_comunica,
    CONCAT(
        RPAD(fields_payload['numero_de_cuenta_870'], 18, '0'),
        LPAD(fields_payload['numero_de_documento'], 11, '0'), 
        (CASE 
            WHEN fields_payload['tipo_de_documento'] = 'tarjeta_cédula_de_extranjería' THEN '4' 
            WHEN fields_payload['tipo_de_documento'] = 'tarjeta_tarjeta_de_identidad' THEN '7'
            ELSE '1' END),
        'E') as code_experian -- genera el codigo experian con la longitud necesaria para crear el masivo por registro
        -- E significa que se baja el reporte por que "Se presento Fraude"
FROM {{database}}.{{table}}
WHERE   true
    and ticket_form_label = 'gestion_de_fraude'
    and fields_payload['numero_de_cuenta_870'] in {{ accounts_list }}
ORDER BY created_at DESC


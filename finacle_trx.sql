select  
        TRIM(numero_producto) AS numero_producto,
        TRIM(numero_identificacion) AS numero_identificacion,
        TRIM(tran_id) as tran_id,
        fecha_posteo, 
        codigo_concepto,
        naturaleza_transaccion,
        valor_transaccion, 
        trans_particular_original

from co_delfos_productos_raw_pdn_rl.transaccional_uso
where   true
        AND (   
            {{ filter_partitions }}
            )
        AND (TRIM(numero_identificacion) IN {{ tuple_numero_documento }} AND TRIM(tran_id) IN {{ tuple_tran_id }})
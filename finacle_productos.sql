SELECT
    trim(both ' ' FROM replace(replace(numero_celular, '+57(', ''), ')', '')) as numero_celular
FROM {{ database }}.{{ table }}
WHERE true
        AND year={{year_co}}
        AND month = {{month_co}}
        AND day = {{day_co}}
        AND CAST(valor_saldo_cuenta as DECIMAL(18, 0)) > 30000000
        ORDER BY CAST(valor_saldo_cuenta as DECIMAL(18, 0)) DESC
limit 1000
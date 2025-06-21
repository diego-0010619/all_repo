
# Lista columnas para separar data remaining y
trx_columns: list = ['numero_documento', 'cantidad_transacciones_reportadas',
               'tran_id_01', 'tran_id_02',
               'tran_id_03', 'tran_id_04',
               'tran_id_05', 'tran_id_06',
               'tran_id_07', 'tran_id_08',
               'tran_id_09', 'tran_id_10',
               'tran_id_11', 'tran_id_12',
               'tran_id_13', 'tran_id_14',
               'tran_id_15', 'tran_id_16',
               'tran_id_17', 'tran_id_18',
               'tran_id_19', 'tran_id_20',
               'fecha_trx_01', 'fecha_trx_02',
               'fecha_trx_03', 'fecha_trx_04',
               'fecha_trx_05', 'fecha_trx_06',
               'fecha_trx_07', 'fecha_trx_08',
               'fecha_trx_09', 'fecha_trx_10',
               'fecha_trx_11', 'fecha_trx_12',
               'fecha_trx_13', 'fecha_trx_14',
               'fecha_trx_15', 'fecha_trx_16',
               'fecha_trx_17', 'fecha_trx_18',
               'fecha_trx_19', 'fecha_trx_20']

# Data para centralizar info de submodalidades soportadas
data_id_pago = [
    {
        'id_pago': 'monto_inferior',
        'numero_de_cuenta_a_debitar': 'BD000001COPP9AG5',
        'tran_code': 'A105',
        'tran_remarks': 'Vulneracion Monto Inferior',
        'prefix_trans_particular_debito': 'Reintegro',
        'flag_date_trans_particular_debito': True,
        'prefix_trans_particular_credito': 'Reintegro',
        'flag_date_trans_particular_credito': True,
        'prefix_trans_particular_2': 'Ticket',
        'flag_number_trans_particular_2': True
    },
    {
        'id_pago': 'impuesto_I003',
        'numero_de_cuenta_a_debitar': 'BD000001COPI8MDY',
        'tran_code': 'I003',
        'tran_remarks': 'Devolucion GMF por Fraude',
        'prefix_trans_particular_debito': 'Devolucion GMF por Fraude',
        'flag_date_trans_particular_debito': False,
        'prefix_trans_particular_credito': 'Devolucion GMF por Fraude',
        'flag_date_trans_particular_credito': False,
        'prefix_trans_particular_2': 'Ticket',
        'flag_number_trans_particular_2': True
    },
    {
        'id_pago': 'enumeración_gestión_fraude',
        'numero_de_cuenta_a_debitar': 'BD000001COPP9AG5',
        'tran_code': 'A017',
        'tran_remarks': 'Vulneracion Enumeracion',
        'prefix_trans_particular_debito': 'Reintegro',
        'flag_date_trans_particular_debito': True,
        'prefix_trans_particular_credito': 'Reintegro',
        'flag_date_trans_particular_credito': True,
        'prefix_trans_particular_2': 'Ticket',
        'flag_number_trans_particular_2': True
    },
    {
        'id_pago': 'reexpedición_de_sim_card_gestión_fraude',
        'numero_de_cuenta_a_debitar': 'BD000001COPP9AG5',
        'tran_code': 'A017',
        'tran_remarks': 'Vulneracion Reexpedicion SIM',
        'prefix_trans_particular_debito': 'Reintegro',
        'flag_date_trans_particular_debito': True,
        'prefix_trans_particular_credito': 'Reintegro',
        'flag_date_trans_particular_credito': True,
        'prefix_trans_particular_2': 'Ticket',
        'flag_number_trans_particular_2': True
    },
    {
        'id_pago': 'vulneración_biométrica_gestión_fraude',
        'numero_de_cuenta_a_debitar': 'BD000001COPP9AG5',
        'tran_code': 'A017',
        'tran_remarks': 'Vulneracion Biometria',
        'prefix_trans_particular_debito': 'Reintegro',
        'flag_date_trans_particular_debito': True,
        'prefix_trans_particular_credito': 'Reintegro',
        'flag_date_trans_particular_credito': True,
        'prefix_trans_particular_2': 'Ticket',
        'flag_number_trans_particular_2': True
    },
    {
        'id_pago': 'vishing_gestión_fraude',
        'numero_de_cuenta_a_debitar': 'BD000001COPP9AG5',
        'tran_code': 'A097',
        'tran_remarks': 'Vulneracion Vishing',
        'prefix_trans_particular_debito': 'Reintegro',
        'flag_date_trans_particular_debito': True,
        'prefix_trans_particular_credito': 'Reintegro',
        'flag_date_trans_particular_credito': True,
        'prefix_trans_particular_2': 'Ticket',
        'flag_number_trans_particular_2': True
    }
]



limite_monto_inferior: int = 200000

list_impuestos = ['I001','I002', 'I003']
list_impuestos_no_fraude = ['I001','I002']

name_columns_masivo = {
    'ticket_id': 'ticket_id',
    'numero_de_cuenta_a_debitar': 'Numero de cuenta a debitar',
    'numero_de_cuenta_a_acreditar': 'Numero de cuenta a acreditar',
    'monto' : 'Monto',
    'trans_particular_debito': 'Trans particular debito',
    'trans_particular_credito': 'Trans particular credito',
    'tran_code': 'Tran code',
    'trans_particular_2': 'Trans particular 2',
    'tran_remarks': 'Tran remarks'




}



# {
#     'id_pago': 'impuesto_I001',
#     'numero_de_cuenta_a_debitar': 'BD000001COPI8MDY',
#     'tran_code': 'I003',
#     'tran_remarks': 'Devolucion GMF por Fraude',
#     'prefix_trans_particular_debito': 'Devolucion GMF por Fraude',
#     'flag_date_trans_particular_debito': False,
#     'prefix_trans_particular_credito': 'Devolucion GMF por Fraude',
#     'flag_date_trans_particular_credito': False,
#     'prefix_trans_particular_2': 'Ticket',
#     'flag_number_trans_particular_2': True
# },
# {
#     'id_pago': 'impuesto_I002',
#     'numero_de_cuenta_a_debitar': 'BD000001COPI8MDY',
#     'tran_code': 'I003',
#     'tran_remarks': 'Devolucion GMF por Fraude',
#     'prefix_trans_particular_debito': 'Devolucion GMF por Fraude',
#     'flag_date_trans_particular_debito': False,
#     'prefix_trans_particular_credito': 'Devolucion GMF por Fraude',
#     'flag_date_trans_particular_credito': False,
#     'prefix_trans_particular_2': 'Ticket',
#     'flag_number_trans_particular_2': True
# },
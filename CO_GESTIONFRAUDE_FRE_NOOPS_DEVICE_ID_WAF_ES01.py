"""
    Este DAG esta disenado para añadir un deviceID al WAF
    equipo_de_trabajo: Gestion Fraude
    version: 0.0.1
"""
import json

from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator
from airflow.providers.amazon.aws.sensors.lambda_function import LambdaFunctionStateSensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

##  ----- DAG PARAMETERS
ENV                         =   Variable.get('env')
DAG_ID                      =   "CO_GESTIONFRAUDE_FRE_NOOPS_DEVICE_ID_WAF_ES01"
DAG_DESCRIPTION             =   """Este DAG esta diseñado para añadir un deviceID al WAF"""
DAG_SCHEDULE                =   None
OPSGENIE_CONNECTION_ID      =   "opsgenie-dataops"
NOTA                        =   "EJECUCION EXITOSA"
AWS_CONN_ID                 =   ""
JIRA_TRANSITIONS_OK         =   "121"
JIRA_TRANSITIONS_FAIL       =   "3"
NOTA_OK                     =   "Ejecutado exitoso"
NOTA_FAIL                   =   "Ejecución Fallida"

## ----- ENV CONDITIONAL
if ENV == "dev":
    ARN_LAMBDA              =   ""
    ACCOUNT                 =   ""
elif ENV == "qa":
    ARN_LAMBDA              =   "arn:aws:lambda:us-east-1:188457381105:function:lambda-infracloud-ts-block-devices-imperva-qa"
    ACCOUNT                 =   "188457381105"
    AWS_CONN_ID             =   "co-tecnologia-noops-lambda-188457381105-aws"
elif ENV == "pdn":
    ARN_LAMBDA              =   ""
    ACCOUNT                 =   ""
    AWS_CONN_ID             =   "co-tecnologia-noops-lambda-268875711053-aws"

def send_error_message_on_opsgenie(context):
    """
    Envia un mensaje de error a través de Slack.

    Args:
        context (dict): contexto de la tarea que contiene información sobre el DAG y la tarea.

    Returns:
        None: Ejecuta la operación de creación de alerta.
    """
    error_header: str = (
        f"""Error de ejecución en el DAG {context.get("task_instance").dag_id}"""
    )
    error_message: str = f"""
        Ha ocurrio un error:
            dag: {context.get("task_instance").dag_id}
            tarea: {context.get("task_instance").task_id}
            url del log: {context.get('task_instance').log_url}
    """
    error_message_on_opsgenie = OpsgenieCreateAlertOperator(
        opsgenie_conn_id    =   OPSGENIE_CONNECTION_ID,
        task_id             =   "error_message_on_opsgenie",
        message             =   error_header,
        description         =   error_message,
    )
    return error_message_on_opsgenie.execute(context=context)

def choose_payload(**kwargs):
    """
    Determina si se agrega o elimina un dispositivo según el parámetro a recibir en params.

    Args:
        **kwargs: Diccionario de argumentos que incluye los parámetros necesarios.

    Returns:
        str: El nombre de la función Lambda a invocar, ya sea 'lambda_invoke_remove' o 'lambda_invoke_add'.
    """

    if kwargs['params']['Accion'] == 'remove':
        return 'accion_Gtasks.lambda_invoke_remove'
    return 'accion_Gtasks.lambda_invoke_add'

def process_description(**kwargs):
    """
    Toma el parámetro description y lo convierte en una lista para retornar el campo payload

    Args:
        **kwargs: Diccionario de argumentos que incluye los parámetros necesarios.

    Returns:
        str: payload con los devaces a bloquear.
    """
    params                  =   kwargs['params']
    devices                 =   params['description'].split(',')
    records                 =   [{"deviceId": device.strip()} for device in devices]
    payload                 =   json.dumps({"records": records})
    return payload

def get_payload(**kwargs):
    """
    Toma el parámetro description y lo convierte en una lista para retornar el campo payload

    Args:
        **kwargs: Diccionario de argumentos que incluye los parámetros necesarios.

    Returns:
        str: payload con los devaces a bloquear.
    """
    payload                 =   process_description(**kwargs)
    kwargs['ti'].xcom_push(key='payload', value=payload)

@dag(
    dag_id              =   DAG_ID,
    description         =   DAG_DESCRIPTION,
    start_date          =   datetime(2024, 1, 1),
    catchup             =   False,
    schedule_interval   =   DAG_SCHEDULE,
    tags=[
        f"evn           :   {ENV}",
        "team           :   GESTIONFRAUDE",
        "dominio        :   GESTIONFRAUDE",
        "subdominio     :   FRE",
        "pais           :   CO",
        "criticidad     :   P3",
    ],
    params={
        "ticket"        :Param("default", type="string", title="ticket"),
        "description"   :Param("default", type="string", title="description"),
        "Accion"        :Param("default", type="string", title="Accion")
    }
)

def workflow():
    """Executes the workflow for the CO_GESTIONFRAUDE_FRE_NOOPS_DEVICE_ID_WAF_ES01 DAG.

    This function defines the tasks and their dependencies for the DAG workflow.
    It consists of two TaskGroups:
    - accion_Gtasks: Contains tasks related to the choice of the action to be executed in the flow.
    - jira_Gtasks: Contains tasks related to the execution on JIRA.

    """
    with TaskGroup("accion_Gtasks") as accion_Gtasks:

        choose_task = BranchPythonOperator(
            task_id             =   'choose_task',
            provide_context     =   True,
            python_callable     =   choose_payload
        )

        process_task = PythonOperator(
            task_id             =   'process_description_task',
            python_callable     =   get_payload,
            provide_context     =   True
        )

        lambda_invoke_add = LambdaInvokeFunctionOperator(
            task_id             =   "lambda_invoke_add",
            function_name       =   ARN_LAMBDA,
            payload             =   "{{ ti.xcom_pull(key='payload', task_ids='accion_Gtasks.process_description_task' ) }}",
            aws_conn_id         =   AWS_CONN_ID
        )

        lambda_invoke_remove = LambdaInvokeFunctionOperator(
            task_id             =   "lambda_invoke_remove",
            function_name       =   ARN_LAMBDA,
            payload             =   "{{ ti.xcom_pull(key='payload', task_ids='accion_Gtasks.process_description_task' ) }}",
            aws_conn_id         =   AWS_CONN_ID
        )

        lambda_sensor_state = LambdaFunctionStateSensor(
            task_id             =   "lambda_sensor_state",
            trigger_rule        =   'none_failed_min_one_success',
            function_name       =   ARN_LAMBDA,
            aws_conn_id         =   AWS_CONN_ID,
            mode                =   'poke',
            timeout             =   (60.0 * 1.0)
        )

        choose_task >> process_task >> [lambda_invoke_add, lambda_invoke_remove] >> lambda_sensor_state

    with TaskGroup("jira_Gtasks") as jira_Gtasks:

        jira_invoke_success = TriggerDagRunOperator(
            task_id             =   "jira_invoke_success",
            trigger_dag_id      =   "CO_TECNOLOGIA_TRANS_NOOPS_JIRA_ES01",
            conf                =   { "ticket":"{{params.ticket}}", "nota_ticket": NOTA_OK, "transitions": JIRA_TRANSITIONS_OK},
            deferrable          =   True,
            wait_for_completion =   True,
            trigger_rule        =   TriggerRule.ALL_SUCCESS
        )

        jira_invoke_failed = TriggerDagRunOperator(
        task_id                 =   "jira_invoke_failed",
        trigger_dag_id          =   "CO_TECNOLOGIA_TRANS_NOOPS_JIRA_ES01",
        conf                    =   {"ticket":"{{params.ticket}}","nota_ticket": NOTA_FAIL, "transitions": JIRA_TRANSITIONS_FAIL},
        deferrable              =   True,
        wait_for_completion     =   True,
        trigger_rule            =   TriggerRule.ALL_FAILED
        )

        [jira_invoke_success, jira_invoke_failed]

    task_inicia_proceso         = EmptyOperator(task_id="inicia_proceso")
    task_finaliza_proceso       = EmptyOperator(task_id="Finaliza_proceso")

    task_inicia_proceso >> accion_Gtasks >> jira_Gtasks >> task_finaliza_proceso

workflow()
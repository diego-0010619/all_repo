from jinja2 import Template
import os

def render_template(name_template:str, **params_query):
    route = os.path.join(os.getcwd(),'extra/utils/templates_sql', name_template)
    # Load template query SQL
    template = Template(open(route, 'r').read())
    # Render template Jinja SQL
    rendered_template = template.render(**params_query)
    return rendered_template

def split_df(df, key_column:str, trx_columns:list):
    df_trx = df[[key_column] + trx_columns]
    remaining_columns = [col for col in df.columns if col != key_column and col not in trx_columns]
    df_remaining = df[[key_column] + remaining_columns]
    return df_trx, df_remaining
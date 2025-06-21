from jinja2 import Template
import os

class SqlRender:

    @staticmethod
    def render_template(name_template:str, **params_query):
        route = os.path.join(os.getcwd(),'utils/templates_sql', name_template)
        # Load template query SQL
        template = Template(open(route, 'r').read())
        # Render template Jinja SQL
        rendered_template = template.render(**params_query)
        return rendered_template
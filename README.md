## Introduccion
El proyecto nace en respuesta a la necesidad del área de datos de integrar en forma organizada y homogenea los jobs de glue
y poder agruparlos en unidades comúnes denominadas proyectos de datos.

**_Nota:_** los jobs que se definen en esta forma crearán al interior una estructura de carpetas por job,

```bash
. DEMO_ConectorJJ_GL
└── src
    ├── addons
    │   ├── __init__.py
    │   ├── params.py
    │   └── project.json      # ( utilizado por NX para el seguimiento de este directorio)
    ├── extra
    │   ├── __init__.py
    │   ├── params.py
    │   └── project.json      # ( utilizado por NX para el seguimiento de este directorio)
    └── job_name
        ├── addons
        │   ├── params.py
        │   └── __init__.py
        ├── extra
        │   ├── params.py
        │   └── __init__.py
        ├── __init__.py
        ├── main.py
        └── project.json      # ( utilizado por NX para el seguimiento de este directorio)
```

## Requerimientos

- Python 3.8 o superior
- Java 11

### Library

El nombre la librería debe ser

- nombredelrepo-rama

ejemplo:

**DEMO_ConectorJJ_GL-develop**

| Name            | Value           |
|-----------------|-----------------|
| aws_bucket_name | nequi-glue-test |
| aws_svc         | sandbox-nq-test |
| environment     | dev             |


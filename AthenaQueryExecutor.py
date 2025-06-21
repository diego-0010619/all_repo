import awswrangler as wr
import pandas as pd

class AthenaQueryExecutor:
    @staticmethod
    def execute_query(query:str, database_athena:str, workgroup_athena:str):
        try:
            df_query = wr.athena.read_sql_query(sql=query, 
                                                database=database_athena, 
                                                workgroup=workgroup_athena,
                                                ctas_approach = False)
            return df_query
        except Exception as e:
            print(f"Error executing query: {e}")
            return None


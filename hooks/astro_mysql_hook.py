from airflow.hooks.mysql_hook import MySqlHook


class AstroMySqlHook(MySqlHook):
    def get_schema(self, table):
        query = \
            """
            SELECT lower(COLUMN_NAME) as COLUMN_NAME, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{0}';
            """.format(table)
        return super().get_records(query)

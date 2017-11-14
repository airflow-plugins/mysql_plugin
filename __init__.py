from airflow.plugins_manager import AirflowPlugin
from mysql_plugin.hooks.astro_mysql_hook import AstroMySqlHook
from mysql_plugin.operators.mysql_to_s3_operator import MySQLToS3Operator
from mysql_plugin.operators.s3_to_mysql_operator import S3ToMySQLOperator


class MySQLToS3Plugin(AirflowPlugin):
    name = "MySQLToS3Plugin"
    operators = [MySQLToS3Operator, S3ToMySQLOperator]
    # Leave in for explicitness
    hooks = [AstroMySqlHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

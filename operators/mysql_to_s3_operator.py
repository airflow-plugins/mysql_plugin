from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from mysql_plugin.hooks.astro_mysql_hook import AstroMySqlHook

from airflow.utils.decorators import apply_defaults
import json
import logging


class MySQLToS3Operator(BaseOperator):
    """
    MySQL to Spreadsheet Operator

    NOTE: When using the MySQLToS3Operator, it is necessary to set the cursor
    to "dictcursor" in the MySQL connection settings within "Extra"
    (e.g.{"cursor":"dictcursor"}). To avoid invalid characters, it is also
    recommended to specify the character encoding (e.g {"charset":"utf8"}).

    NOTE: Because this operator accesses a single database via concurrent
    connections, it is advised that a connection pool be used to control
    requests. - https://airflow.incubator.apache.org/concepts.html#pools

    :param mysql_conn_id:           The input mysql connection id.
    :type mysql_conn_id:            string
    :param mysql_table:             The input MySQL table to pull data from.
    :type mysql_table:              string
    :param s3_conn_id:              The destination s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The destination s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The destination s3 key.
    :type s3_key:                   string
    :param package_schema:          *(optional)* Whether or not to pull the
                                    schema information for the table as well as
                                    the data.
    :type package_schema:           boolean
    :param incremental_key:         *(optional)* The incrementing key to filter
                                    the source data with. Currently only
                                    accepts a column with type of timestamp.
    :type incremental_key:          string
    :param start:                   *(optional)* The start date to filter
                                    records with based on the incremental_key.
                                    Only required if using the incremental_key
                                    field.
    :type start:                    timestamp (YYYY-MM-DD HH:MM:SS)
    :param end:                     *(optional)* The end date to filter
                                    records with based on the incremental_key.
                                    Only required if using the incremental_key
                                    field.
    :type end:                       timestamp (YYYY-MM-DD HH:MM:SS)
    """

    template_fields = ['start', 'end', 's3_key']

    @apply_defaults
    def __init__(self,
                 mysql_conn_id,
                 mysql_table,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 package_schema=False,
                 incremental_key=None,
                 start=None,
                 end=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.mysql_table = mysql_table
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.package_schema = package_schema
        self.incremental_key = incremental_key
        self.start = start
        self.end = end

    def execute(self, context):
        hook = AstroMySqlHook(self.mysql_conn_id)
        self.get_records(hook)
        if self.package_schema:
            self.get_schema(hook, self.mysql_table)

    def get_schema(self, hook, table):
        logging.info('Initiating schema retrieval.')
        results = list(hook.get_schema(table))
        output_dict = {}
        for i in results:
            new = []
            new_dict = {}
            for n in i:
                if n == 'COLUMN_NAME':
                    new.insert(0, i[n])
                else:
                    new.insert(1, i[n])
            new = [i for i in new if i.islower()]
            if len(new) == 2:
                new_dict[new[0]] = new[1]
                output_dict.update(new_dict)
        self.s3_upload(str(output_dict), schema=True)

    def get_records(self, hook):
        logging.info('Initiating record retrieval.')
        logging.info('Start Date: {0}'.format(self.start))
        logging.info('End Date: {0}'.format(self.end))

        if all([self.incremental_key, self.start, self.end]):
            query_filter = """ WHERE {0} >= '{1}' AND {0} < '{2}'
                """.format(self.incremental_key, self.start, self.end)

        if all([self.incremental_key, self.start]) and not self.end:
            query_filter = """ WHERE {0} >= '{1}'
                """.format(self.incremental_key, self.start)

        if not self.incremental_key:
            query_filter = ''

        query = \
            """
            SELECT *
            FROM {0}
            {1}
            """.format(self.mysql_table, query_filter)

        # Perform query and convert returned tuple to list
        results = list(hook.get_records(query))
        logging.info('Successfully performed query.')

        # Iterate through list of dictionaries (one dict per row queried)
        # and convert datetime and date values to isoformat.
        # (e.g. datetime(2017, 08, 01) --> "2017-08-01T00:00:00")
        results = [dict([k, str(v)] if v is not None else [k, v]
                   for k, v in i.items()) for i in results]
        results = '\n'.join([json.dumps(i) for i in results])
        self.s3_upload(results)
        return results

    def s3_upload(self, results, schema=False):
        s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        key = '{0}'.format(self.s3_key)
        # If the file being uploaded to s3 is a schema, append "_schema" to the
        # end of the file name.
        if schema and key[-5:] == '.json':
            key = key[:-5] + '_schema' + key[-5:]
        if schema and key[-4:] == '.csv':
            key = key[:-4] + '_schema' + key[-4:]
        s3.load_string(
            string_data=results,
            bucket_name=self.s3_bucket,
            key=key,
            replace=True
        )
        s3.connection.close()
        logging.info('File uploaded to s3')

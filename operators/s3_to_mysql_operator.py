from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.mysql_hook import MySqlHook
import dateutil.parser
import json
import logging


class S3ToMySQLOperator(BaseOperator):
    """

    NOTE: To avoid invalid characters, it is recommended
    to specify the character encoding (e.g {"charset":"utf8"}).

    S3 To MySQL Operator
    
    :param s3_conn_id:              The source s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The source s3 key.
    :type s3_key:                   string
    :param mysql_conn_id:           The destination redshift connection id.
    :type mysql_conn_id:            string
    :param database:                The destination database name.
    :type database:                 string
    :param table:                   The destination mysql table name.
    :type table:                    string
    :param field_schema:            An array of dicts in the following format:
                                    {'name': 'column_name', 'type': 'int(11)'}
                                    which determine what fields will be created
                                    and inserted.
    :type field_schema:             array
    :param primary_key:             The primary key for the
                                    destination table. Multiple strings in the
                                    array signify a compound key.
    :type primary_key:              array
    :param incremental_key:         *(optional)* The incremental key to compare
                                    new data against the destination table
                                    with. Only required if using a load_type of
                                    "upsert".
    :type incremental_key:          string
    :param load_type:               The method of loading into MySQL that
                                    should occur. Options are "append",
                                    "rebuild", and "upsert". Defaults to
                                    "append."
    :type load_type:                string
    """

    template_fields = ('s3_key',)

    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 mysql_conn_id,
                 database,
                 table,
                 field_schema,
                 primary_key=[],
                 incremental_key=None,
                 load_type='append',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.mysql_conn_id = mysql_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.database = database
        self.field_schema = field_schema
        self.primary_key = primary_key
        self.incremental_key = incremental_key
        self.load_type = load_type

    def execute(self, context):
        m_hook = MySqlHook(self.mysql_conn_id)

        data = (S3Hook(self.s3_conn_id)
                .get_key(self.s3_key, bucket_name=self.s3_bucket)
                .get_contents_as_string(encoding='utf-8'))

        self.copy_data(m_hook, data)

    def copy_data(self, m_hook, data):
        if self.load_type == 'rebuild':
            drop_query = \
                """
                DROP TABLE IF EXISTS {schema}.{table}
                """.format(schema=self.database, table=self.table)
            m_hook.run(drop_query)

        table_exists_query = \
            """
            SELECT *
            FROM information_schema.tables
            WHERE table_schema = '{database}' AND table_name = '{table}'
            """.format(database=self.database, table=self.table)

        if not m_hook.get_records(table_exists_query):
            self.create_table(m_hook)
        else:
            self.reconcile_schemas(m_hook)

        self.write_data(m_hook, data)

    def create_table(self, m_hook):
        # Fields are surround by `` in order to avoid namespace conflicts
        # with reserved words in MySQL.
        # https://dev.mysql.com/doc/refman/5.7/en/identifiers.html

        fields = ['`{name}` {type} {nullable}'.format(name=field['name'],
                                                      type=field['type'],
                                                      nullable='NOT NULL'
                                                      if field['name']
                                                      in self.primary_key
                                                      else 'NULL')
                  for field in self.field_schema]

        keys = ', '.join(self.primary_key)

        create_query = \
                    """
                    CREATE TABLE IF NOT EXISTS {schema}.{table} ({fields}
                    """.format(schema=self.database,
                               table=self.table,
                               fields=', '.join(fields))
        if keys:
            create_query += ', PRIMARY KEY (`{keys}`)'.format(keys=keys)

        create_query += ')'

        m_hook.run(create_query)

    def reconcile_schemas(self, m_hook):
        describe_query = 'DESCRIBE {schema}.{table}'.format(schema=self.database,
                                                            table=self.table)
        records = m_hook.get_records(describe_query)
        existing_columns_names = [x[0] for x in records]
        incoming_column_names = [field['name'] for field in self.field_schema]
        missing_columns = list(set(incoming_column_names) -
                               set(existing_columns_names))
        if len(missing_columns):
            columns = ['ADD COLUMN {name} {type} NULL'.format(name=field['name'],
                                                              type=field['type'])
                       for field in self.field_schema
                       if field['name'] in missing_columns]

            alter_query = \
                """
                ALTER TABLE {schema}.{table} {columns}
                """.format(schema=self.database,
                           table=self.table,
                           columns=', '.join(columns))

            m_hook.run(alter_query)
            logging.info('The new columns were:' + str(missing_columns))
        else:
            logging.info('There were no new columns.')

    def write_data(self, m_hook, data):
        fields = ', '.join([field['name'] for field in self.field_schema])

        placeholders = ', '.join('%({name})s'.format(name=field['name'])
                                 for field in self.field_schema)

        insert_query = \
            """
            INSERT INTO {schema}.{table} ({columns})
            VALUES ({placeholders})
            """.format(schema=self.database,
                       table=self.table,
                       columns=fields,
                       placeholders=placeholders)

        if self.load_type == 'upsert':
            # Add IF check to ensure that the records being inserted have an
            # incremental_key with a value greater than the existing records.
            update_set = ', '.join(["""
                                    {name} = IF({ik} < VALUES({ik}),
                                    VALUES({name}), {name})
                                    """.format(name=field['name'],
                                               ik=self.incremental_key)
                                    for field in self.field_schema])

            insert_query += ('ON DUPLICATE KEY UPDATE {update_set}'
                             .format(update_set=update_set))

        # Split the incoming JSON newlines string along new lines.
        # Remove cases where two or more '\n' results in empty entries.
        records = [record for record in data.split('\n') if record]

        # Create a default "record" object with all available fields
        # intialized to None. These will be overwritten with the proper
        # field values as available.

        default_object = {}

        for field in self.field_schema:
            default_object[field['name']] = None

        # Initialize null to Nonetype for incoming null values in records dict
        null = None
        output = []

        for record in records:
            line_object = default_object.copy()
            line_object.update(json.loads(record))
            output.append(line_object)

        date_fields = [field['name'] for field in self.field_schema if field['type'] in ['datetime', 'date']]

        def convert_timestamps(key, value):
            if key in date_fields:
                try:
                    # Parse strings to look for values that match a timestamp
                    # and convert to datetime.
                    # Set ignoretz=False to keep timezones embedded in datetime.
                    # http://bit.ly/2zwcebe
                    value = dateutil.parser.parse(value, ignoretz=False)
                    return value
                except (ValueError, TypeError, OverflowError):
                    # If the value does not match a timestamp or is null,
                    # return intial value.
                    return value
            else:
                return value

        output = [dict([k, convert_timestamps(k, v)] if v is not None else [k, v]
                  for k, v in i.items()) for i in output]

        conn = m_hook.get_conn()
        cur = conn.cursor()
        cur.executemany(insert_query, output)
        cur.close()
        conn.commit()
        conn.close()

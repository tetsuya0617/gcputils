import os
import json
from google.cloud import bigquery


class BigQueryAccessor:
    def __init__(self):
        """
        Create Big Query Client
        """
        if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
            raise Exception("Set environment variable　as GOOGLE_APPLICATION_CREDENTIALS")
        elif not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            raise Exception("Set correct credential key file path in GOOGLE_APPLICATION_CREDENTIALS ")
        elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            # Local
            json_key_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            self.bq_client = bigquery.Client.from_service_account_json(json_key_file)
        else:
            # Google Kubernetes Engine
            self.bq_client = bigquery.Client()

    def get_dataset(self):
        """
        Check dataset exsistence
        """
        datasets = [dataset.dataset_id for dataset in self.bq_client.list_datasets()]
        return datasets

    def get_table_name(self, dataset):
        """
        Check table exsistence
        """
        try:
            table_names = [table.table_id for table in self.bq_client.list_tables(dataset)]
        except Exception:
            raise Exception('Dataset is not correct name')
        return table_names

    def create_table_from_json(self, path_schema_file, dataset, table_name):
        """
        Create table on bigquery based on schema json file
        """
        if not path_schema_file:
            raise Exception('Path to schema file is not correct or it does not exist')
        with open(path_schema_file, 'r') as f:
            table_schema = json.load(f)

        if dataset not in self.get_dataset():
            raise Exception('Dataset was not found')

        if table_name in self.get_table_name(dataset):
            raise Exception('The table already exists in bigquery')

        SCHEMA = []
        for schema in table_schema:
            name = schema['name']
            type = schema['type']
            if 'mode' in list(tuple(schema)):
                mode = schema['mode']
            else:
                mode = 'NULLABLE'

            if 'description' in list(tuple(schema)):
                description = schema['description']
            else:
                description = None

            if 'fields' in list(tuple(schema)):
                fields = schema['fields']
            else:
                fields = ()
            SCHEMA.append(bigquery.SchemaField(name, type, mode=mode, description=description, fields=fields))

        dataset_ref = self.bq_client.dataset(dataset)
        dataset = bigquery.Dataset(dataset_ref)
        table_ref = dataset.table(table_name)
        table = bigquery.Table(table_ref, schema=SCHEMA)

        try:
            self.bq_client.create_table(table)
        except Exception:
            raise Exception('Table could successfully not be created on bigquery')
        return 'Table could successfully be created on bigquery'

    def load_data_from_gcs(self,
                           dataset,
                           uris,
                           table_name,
                           location='US',
                           skip_leading_rows=0,
                           source_format='CSV',
                           create_disposition='CREATE_NEVER',
                           write_disposition='WRITE_EMPTY'):
        """
        ---------------------------------------------------
        Load data from gcs (You have to upload file on gcs.).
        ---------------------------------------------------

        params: skip_leading_rows
        Number of rows to skip when reading data (CSV only).

        params: source_format
        'AVRO': Specifies Avro format.
        'CSV': Specifies CSV format.
        'DATASTORE_BACKUP': Specifies datastore backup format
        'NEWLINE_DELIMITED_JSON': Specifies newline delimited JSON format.
        'ORC': Specifies Orc format.
        'PARQUET': Specifies Parquet format.

         params: create_disposition
        'CREATE_IF_NEEDED': If the table does not exist, BigQuery creates the table.
        'CREATE_NEVER': The table must already exist. If it does not, a ‘notFound’ error is returned in the job result.

        prams: write_disposition
        'WRITE_APPEND': If the table already exists, BigQuery appends the data to the table.
        'WRITE_EMPTY': If the table already exists and contains data, a ‘duplicate’ error is returned in the job result.
        'WRITE_TRUNCATE': If the table already exists, BigQuery overwrites the table data.
        """

        dataset_ref = self.bq_client.dataset(dataset)
        dataset = bigquery.Dataset(dataset_ref)
        table_ref = dataset.table(table_name)
        uris = uris

        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = skip_leading_rows
        job_config.create_disposition = create_disposition
        job_config.source_format = source_format
        job_config.write_disposition = write_disposition

        load_job = self.bq_client.load_table_from_uri(
            uris,
            table_ref,
            location=location,
            job_config=job_config,
        )

        try:
            load_job.result()
        except Exception:
            raise Exception('Table could successfully not be loaded on bigquery')
        return 'Loading data is finished'

    def execute_query(self, query, timeout=30):
        query =(query)
        query_job = self.bq_client.query(query, location='US')
        iterator = query_job.result(timeout=timeout)
        rows = list(iterator)
        return rows

    def execute_params_query(self, query, param_list, timeout=30):
        params = [bigquery.ScalarQueryParameter(data_name, data_type, value) for data_name, data_type, value in param_list]

        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = params
        query_job = self.bq_client.query(query, job_config=job_config)

        # Waits for job to complete.
        results = list(query_job.result(timeout=timeout))
        values = [list(x.values()) for x in results]

        return values
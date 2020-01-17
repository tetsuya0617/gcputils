# [bq_accessor.py]
#
# Copyright (c) [2019] [Tetsuya Hirata]
#
# This software is released under the MIT License.
# http://opensource.org/licenses/mit-license.php

import os
import json
from google.cloud import bigquery


class BigQueryAccessor:
    def __init__(self):
        """
        Create Big Query Client
        """
        if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            # Local
            if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                raise Exception("Set correct credential key file path in GOOGLE_APPLICATION_CREDENTIALS ")
            else:
                json_key_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
                self.bq_client = bigquery.Client.from_service_account_json(json_key_file)
        else:
            # Google Kubernetes Engine
            self.bq_client = bigquery.Client()

    def get_dataset(self):
        """
        Get datasets if datasets do not exist, then return empty list
        """
        datasets = [dataset.dataset_id for dataset in self.bq_client.list_datasets()]
        return datasets

    def get_table_name(self, dataset):
        """
        Get table names if table names do not exist, then return exception error message
        """
        try:
            table_names = [table.table_id for table in self.bq_client.list_tables(dataset)]
        except Exception:
            raise Exception("Dataset name is not correct")
        return table_names

    def check_if_dataset_exists(self, dataset):
        """
        Check if dataset exsists in gcs and then return True or False
        """
        datasets = self.get_dataset()
        return True if dataset in datasets else False

    def check_if_table_exists(self, dataset, table_name):
        """
        Check if table exsists in gcs and then return True or False
        """        
        table_names = self.get_table_name(dataset)
        return True if table_name in table_names else False

    def create_table_from_json(self, path_schema_file, dataset, table_name):
        """
        Create table on bigquery based on schema json file
        """
        if not path_schema_file:
            raise Exception("Path to schema file is not correct or it does not exist")
        with open(path_schema_file, "r") as f:
            table_schema = json.load(f)

        if dataset not in self.get_dataset():
            raise Exception("Dataset was not found")

        if table_name in self.get_table_name(dataset):
            raise Exception("The table already exists in bigquery")

        SCHEMA = []
        for schema in table_schema:
            name = schema["name"]
            type = schema["type"]
            if "mode" in list(tuple(schema)):
                mode = schema["mode"]
            else:
                mode = "NULLABLE"

            if "description" in list(tuple(schema)):
                description = schema["description"]
            else:
                description = None

            if "fields" in list(tuple(schema)):
                fields = schema["fields"]
            else:
                fields = ()
            SCHEMA.append(
                bigquery.SchemaField(name, type, mode=mode, description=description, fields=fields)
            )

        dataset_ref = self.bq_client.dataset(dataset)
        dataset = bigquery.Dataset(dataset_ref)
        table_ref = dataset.table(table_name)
        table = bigquery.Table(table_ref, schema=SCHEMA)

        try:
            self.bq_client.create_table(table)
        except Exception:
            raise Exception("Table could not successfully be created on bigquery")
        return "Table could successfully be created on bigquery"

    def load_data_from_gcs(
        self,
        dataset,
        uris,
        table_name,
        location="US",
        skip_leading_rows=0,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_EMPTY",
    ):
        """Load data from gcs (You have to upload file on gcs.).

        Args:
            dataset(str): ID of dataset containing the table.
            uris(str):URIs of data files to be loaded; in format "gs://.../..."
            table_name(str): The ID of the table (=table name on bigquery loaded from the data on gcs)
            location(str): Location where to run the job. Must match the location of the destination table.
            skip_leading_rows: Number of rows to skip when reading data (CSV only).
            source_format:
                   'AVRO': Specifies Avro format.
                   'CSV': Specifies CSV format.
                   'DATASTORE_BACKUP': Specifies datastore backup format
                   'NEWLINE_DELIMITED_JSON': Specifies newline delimited JSON format.
                   'ORC': Specifies Orc format.
                   'PARQUET': Specifies Parquet format.
            create_disposition:
                   'CREATE_IF_NEEDED': If the table does not exist, BigQuery creates the table.
                   'CREATE_NEVER': The table must already exist. If it does not, a ‘notFound’ error is returned in the job result.

            write_disposition:
                   'WRITE_APPEND': If the table already exists, BigQuery appends the data to the table.
                   'WRITE_EMPTY': If the table already exists and contains data, a ‘duplicate’ error is returned in the job result.
                   'WRITE_TRUNCATE': If the table already exists, BigQuery overwrites the table data.
        Notes:
            The above parameter description and parametres are referred to the URL
            https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.LoadJobConfig.html

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
            uris, table_ref, location=location, job_config=job_config
        )

        try:
            load_job.result()
        except Exception:
            raise Exception("Table could successfully not be loaded on bigquery")
        return "Loading data is finished"

    def execute_query(
        self,
        query,
        job_id=None,
        job_id_prefix=None,
        location="US",
        timeout=30,
        page_size=0,
        project=None,
        allow_large_results=False,
        destination=None,
        destination_encryption_configuration=None,
        dry_run=False,
        labels=None,
        priority=None,
        query_parameters=None,
        schema_update_options=None,
        table_definitions=None,
        time_partitioning=None,
        udf_resources=None,
        use_legacy_sql=False,
        use_query_cache=False,
        write_disposition=None,
    ):
        """Execute a simple query or query with the below parameters

        Args:
            query(str): SQL query to be executed. Defaults to the standard SQL dialect. Use the job_config parameter to change dialects.
            job_id(str): ID to use for the query job.
            job_id_prefix(str): The prefix to use for a randomly generated job ID. This parameter will be ignored if a job_id is also given.
            location(str): Location where to run the job. Must match the location of the any table used in the query as well as the destination table.
            project(str): Project ID of the project of where to run the job. Defaults to the client’s project.
            allow_large_results(bool): Allow large query results tables (legacy SQL, only)
            destination:table where results are written or None if not set.
            destination_encryption_configuration:Custom encryption configuration for the destination table.
            dry_run(bool):True if this query should be a dry run to estimate costs.
            labels(Dict[str, str]):Labels for the job.
            priority:Priority of the query.
            query_parameters(list[list, tuple]): list of parameters for parameterized query (empty by default)
            schema_update_options: Specifies updates to the destination table schema to allow as a side effect of the query job.
            table_definitions: Definitions for external tables or None if not set.
            time_partitioning: Specifies time-based partitioning for the destination table.
            udf_resources: user defined function resources (empty by default)
            use_legacy_sql(bool): Use legacy SQL syntax.
            use_query_cache(bool): Look for the query result in the cache.
            write_disposition: Action that occurs if the destination table already exists.
            timeout(float): How long (in seconds) to wait for job to complete before raising a concurrent.futures.TimeoutError.
            page_size(int): The maximum number of rows in each page of results from this request. Non-positive values are ignored.

        Returns:
            Two-dimensional array: the result values from bigquery

        Notes:
            The above parameter description and parametres are referred to the URL
            https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.client.Client.query.html
            https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.QueryJob.html#google.cloud.bigquery.job.QueryJob

            job_config: Extra configuration options for the job.
        """
        job_config = bigquery.QueryJobConfig()
        if query_parameters:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(data_name, data_type, value)
                for data_name, data_type, value in query_parameters
            ]
        job_config.allow_large_results = allow_large_results
        job_config.destination = destination
        job_config.destination_encryption_configuration = destination_encryption_configuration
        job_config.dry_run = dry_run
        if labels:
            job_config.labels = labels
        job_config.priority = priority
        if schema_update_options:
            job_config.schema_update_options = schema_update_options
        if table_definitions:
            job_config.table_definitions = table_definitions
        if time_partitioning:
            job_config.time_partitioning = time_partitioning
        if udf_resources:
            job_config.udf_resources = udf_resources
        job_config.use_legacy_sql = use_legacy_sql
        if use_query_cache:
            job_config.use_query_cache = use_query_cache
        if write_disposition:
            job_config.write_disposition = write_disposition

        query_job = self.bq_client.query(
            query,
            job_id=job_id,
            project=project,
            job_id_prefix=job_id_prefix,
            job_config=job_config,
            location=location,
        )

        # Output of query results and values
        results = list(query_job.result(timeout=timeout, page_size=page_size))
        values = [list(x.values()) for x in results]
        return values

This software is released under the MIT License, see LICENSE.txt.


`gcp-accessor` is a wrapper of [Google Cloud Storage API](https://cloud.google.com/storage/) and [Google BigQuery API](https://cloud.google.com/bigquery/what-is-bigquery) for more simply accesseing both functions.  

While there are [google-cloud-storage](https://github.com/googleapis/google-cloud-python/tree/master/storage/) and [google-cloud-bigquery](https://github.com/googleapis/google-cloud-python) library provided by Google which have several sophisticated features, frequently used features can be limited in python application development.  

`gcp-accessor` focuses on the simplicity of usage pattern and includes the selected features as described below.  

***
- bq_accessor
  - get_dataset
  - get_table_name
  - check_if_dataset_exists
  - check_if_table_exists
  - create_table_from_json
  - load_data_from_gcs
  - execute_query
- gcs_accessor
  - get_blob
  - get_blob_list
  - get_uris_list
  - upload_csv_gzip
  - download_csv_gzip

***
Setup
-
#### Installation
```bash
pip install gcp-accessor
```

#### Set GOOGLE_APPLICATION_CREDENTIALS
```bash
export GOOGLE_APPLICATION_CREDENTIALS='full path to credential key json file'
```

Usage of Google Cloud Storage Accessor
-
#### Import gcp_accessor and create google cloud storage client
```python
import gcp_accessor
gcs = gcp_accessor.GoogleCloudStorageAccessor()
```

#### Get Binary Large Object (blob) from google cloud storage (gcs)
```python
gcs.get_blob('bucket_name', 'full path to file on gcs')
```

#### Get Binary Large Object (blob) lists as HTTPIterator object from gcs
```python
gcs.get_blob_list('bucket_name', prefix=None, delimiter=None)
```

#### Get uris lists
```python
gcs.get_uris_list('bucket_name', prefix=None, delimiter=None)
```

#### Upload gzip object on gcs
```python
gcs.upload_csv_gzip('bucket_name', 'full path to file on gcs', 'texts')
```

#### Download gzip file from gcs and load it through in-memory
```python
gcs.download_csv_gzip('bucket_name', 'full path to file on gcs')
```

Usage of Big Query Accessor
-
#### Import gcp_accessor and create big query client
```python
import gcp_accessor
bq = gcp_accessor.BigQueryAccessor()
```
#### Get datasets if datasets do not exist, then return empty list

```python
bq.get_dataset()
```

#### Get table names if table names do not exist, then return exception error message
```python
bq.get_table_name('dataset_name')
```
#### Check if dataset exsists in bigquery and then return True or False
```python
bq.check_if_dataset_exists('dataset_name')
```

#### Check if table exsists in bigquery and then return True or False
```python
bq.check_if_table_exists('dataset_name', 'table_name')
```


#### Create table on bigquery based on schema json file
```python
bq.create_table_from_json('path_schema_file', 'dataset', 'table_name')
```

#### Load data from gcs (You have to upload file on gcs.).
```python
bq.load_data_from_gcs(
        'dataset_name',
        'uris',
        'table_name',
        location="US",
        skip_leading_rows=0,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_EMPTY",
    )
```

#### Execute a simple query or query with the below optipons
```python
bq.execute_query(
        'query',
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
        write_disposition=None
    )
```



Note
-
Some argument names and descriptions about each argument are cited and referred from the documents of ['Google Cloud Client Libraries for Python'](https://googleapis.github.io/google-cloud-python/latest/index.html) The explanations about each argument are written in the code.

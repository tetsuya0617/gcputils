This software is released under the MIT License, see LICENSE.txt.


`google-cloud-accessor` is a wrapper of [Google Cloud Storage API](https://cloud.google.com/storage/) and [Google BigQuery API](https://cloud.google.com/bigquery/what-is-bigquery) for more simply accesseing both functions.  

While there are [google-cloud-storage](https://github.com/googleapis/google-cloud-python/tree/master/storage/) and [google-cloud-bigquery](https://github.com/googleapis/google-cloud-python) library provided by Google which have several sophisticated features, frequently used features can be limited in python application development.  

`google-cloud-accessor` focuses on the simplicity of usage pattern and includes the selected features as described below.  

***
- bq_accessor
  - get_dataset
  - get_table_name
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
pip install google-cloud-accessor
```

#### Set GOOGLE_APPLICATION_CREDENTIALS
```bash
export GOOGLE_APPLICATION_CREDENTIALS='full path to credential key json file'
```

Usage of Google Cloud Storage Accessor
-
#### Import GoogleCloudStorageAccessor object 
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
gcs.get_blob_list('bucket_name', prefix, delimiter)
```

#### Get uris lists
```python
gcs.get_uris_list('bucket_name', prefix, delimiter)
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
#### Import BigQueryAccessor object 
```python
import gcp_accessor
bq = gcp_accessor.BigQueryAccessor()
```
#### Check dataset exsistence
```python
bq.get_dataset()
```

#### Check table exsistence
```python
bq.get_table_name(dataset)
```

#### Create table on bigquery based on schema json file
```python
bq.create_table_from_json(path_schema_file, dataset, table_name)
```

#### Load data from gcs (You have to upload file on gcs.).
```python
bq.load_data_from_gcs(
        dataset,
        uris,
        table_name,
        location="US",
        skip_leading_rows=0,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_EMPTY",
    )
```

#### Execute a simple query or query with the below parameters
```python
bq.execute_query(
        query,
        location="US",
        timeout=30,
        page_size=0
    )
```

Note
-
Some argument names and descriptions about each argument are cited and referred from the documents of ['Google Cloud Client Libraries for Python'](https://googleapis.github.io/google-cloud-python/latest/index.html)

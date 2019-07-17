This software is released under the MIT License, see LICENSE.txt.


`google-cloud-accessor` is a wrapper of [Google Cloud Storage API](https://cloud.google.com/storage/) and [Google BigQuery API](https://cloud.google.com/bigquery/what-is-bigquery) for more simply accesseing both functions.  

While there are [google-cloud-storage](https://github.com/googleapis/google-cloud-python/tree/master/storage/) and [google-cloud-bigquery](https://github.com/googleapis/google-cloud-python) library provided by Google which have several sophisticated features, frequently used features can be limited in python application development.  

`google-cloud-accessor` focuses on the simplicity of usage pattern and includes the selected features as described below.  

- bq_accessor
- gcs_accessor
  - get_blob
  - get_blob_list
  - get_uris_list
  - upload_csv_gzip
  - download_csv_gzip

Usage
-
#### Installation
```bash
pip install google-cloud-accessor
```

#### Set GOOGLE_APPLICATION_CREDENTIALS
```bash
export GOOGLE_APPLICATION_CREDENTIALS='full path to credential key json file'
```

#### Import GoogleComputeStorageAccessor object 
```python
import gcp_accessor
gcs = gcp_accessor.GoogleComputeStorageAccessor()
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

Documentation
-

Setup
-

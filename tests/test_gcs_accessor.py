from gcp_accessor.gcs_accessor import GoogleComputeStorageAccessor


def test_get_blob():
    bucket_name = 'learning-video'
    storage_path = 'dummy/dummy/dummy.csv.gz'
    blob = GoogleComputeStorageAccessor().get_blob(bucket_name, storage_path)
    print(blob)
    print(type(blob))
    assert isinstance(blob, object)


def test_blob_lists():
    bucket_name = 'learning-video'
    prefix = 'dummy/dummy/'
    delimiter = '/'

    blob_list = GoogleComputeStorageAccessor().get_blob_list(bucket_name, prefix, delimiter)
    print(blob_list)
    print(type(blob_list))
    assert isinstance(blob_list, object)


def test_uris_list():
    bucket_name = 'learning-video'
    prefix = 'dummy/dummy/'
    delimiter = '/'

    uris_lists = GoogleComputeStorageAccessor().get_uris_list(bucket_name, prefix, delimiter)

    assert len(uris_lists) <= 3


def test_upload_csv_gzip():
    bucket_name = 'learning-video'
    storage_path = 'dummy/dummy/upload_dummy.csv.gz'
    # テストデータ
    texts = [('Math', '00001OR1MO1323602', (-0.5288827783075652, -0.48843161951992525)),
                 ('Math', '00001OR1MO1131401', (0.1519841773576938, 3.3056775366135884)),
                 ('Math', '00001OR1MO1D20112', (0.6078849552839164, 1.0228835119323234)),
                 ('Math', '00001OR1MO1A25501', (0.24435182526829782, 0.060129186619730976)),
                 ('Math', '00001OR1MO1D20109', (0.004650449495107372, -144.69777442920537))]

    res = GoogleComputeStorageAccessor().upload_csv_gzip(bucket_name, storage_path, texts)
    assert res == ({'message': 'Gzip file is successfully uploaded on gcs', 'result': 200}, 200)


def test_download_csv_gzip():
    bucket_name = 'learning-video'
    storage_path = 'dummy/dummy/dummy.csv.gz'

    text = GoogleComputeStorageAccessor().download_csv_gzip(bucket_name, storage_path)
    assert isinstance(text, list)



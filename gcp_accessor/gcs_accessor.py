# [gcs_accessor.py]
#
# Copyright (c) [2019] [Tetsuya Hirata]
#
# This software is released under the MIT License.
# http://opensource.org/licenses/mit-license.php

import io, os, csv, gzip
from google.cloud import storage


class GoogleCloudStorageAccessor:
    def __init__(self):
        """
        Create Google Cloud Storage Client
        """
        if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            # Local
            if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                raise Exception("Set correct credential key file path in GOOGLE_APPLICATION_CREDENTIALS ")
            else:
                json_key_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
                self.storage_client = storage.Client.from_service_account_json(json_key_file)
        else:
            # Google Kubernetes Engine
            self.storage_client = storage.Client()

    def get_blob(self, bucket_name, storage_path):
        """
        Get blob(Binary Large Object) from gcs
        """
        if not bucket_name:
            raise Exception("bucket_name was not found")
        bucket = self.storage_client.get_bucket(bucket_name)

        if not storage_path:
            raise Exception("storage_path was not found")
        blob = bucket.blob(storage_path)
        return blob

    def get_blob_list(self, bucket_name, prefix=None, delimiter=None):
        """
        Get blob(Binary Large Object) lists as HTTPIterator object from gcs
        """
        if not bucket_name:
            raise Exception("bucket_name was not found")
        bucket = self.storage_client.get_bucket(bucket_name)
        list_blob = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
        if not list_blob:
            raise Exception("list_blob was empty")
        return list_blob

    def get_uris_list(self, bucket_name, prefix=None, delimiter=None):
        list_blob = self.get_blob_list(bucket_name, prefix=prefix, delimiter=delimiter)
        uris_lists = ["gs://" + bucket_name + "/" + blob.name for blob in list_blob]
        return uris_lists

    def upload_csv_gzip(self, bucket_name, storage_path, texts):
        """
        Upload gzip object on gcs
        """
        if not texts:
            raise Exception("texts were empty")

        with io.StringIO() as csv_obj:
            writer = csv.writer(csv_obj, quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n")
            writer.writerows(texts)
            csv_texts = csv_obj.getvalue()

        with io.BytesIO() as gzip_obj:
            with gzip.GzipFile(fileobj=gzip_obj, mode="wb") as gzip_file:
                bytes_f = csv_texts.encode()
                gzip_file.write(bytes_f)
                blob = self.get_blob(bucket_name, storage_path)
            try:
                blob.upload_from_file(gzip_obj, rewind=True, content_type="application/gzip")
            except Exception:
                raise Exception("Gzip object could not be uploaded on gcs")
        return {"message": "Gzip file is successfully uploaded on gcs", "result": 200}, 200

    def download_csv_gzip(self, bucket_name, storage_path):
        """
        Download gzip file from gcs and load it through in-memory
        """
        blob = self.get_blob(bucket_name, storage_path)
        gzip_obj = io.BytesIO()

        try:
            blob.download_to_file(gzip_obj)
        except Exception:
            raise Exception("Gzip object could not be downloaded in in-memory")
        gzip_obj.seek(0)

        try:
            with gzip.GzipFile(fileobj=gzip_obj, mode="r") as gzip_file:
                with io.TextIOWrapper(gzip_file, encoding="utf-8") as readable_file:
                    csv_texts = csv.reader(
                        readable_file, quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n"
                    )
                    text = [texts for texts in csv_texts]
        except Exception:
            raise Exception("Gzip object could not be loaded")
        gzip_obj.close()
        return text

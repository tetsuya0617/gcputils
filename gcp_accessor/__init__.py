from pkg_resources import get_distribution

__version__ = get_distribution("gcp-accessor").version

from gcp_accessor.gcs_accessor import GoogleCloudStorageAccessor
from gcp_accessor.bq_accessor import BigQueryAccessor

__all__ = ["__version__", "GoogleCloudStorageAccessor", "BigQueryAccessor"]
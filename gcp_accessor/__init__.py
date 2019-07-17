from pkg_resources import get_distribution

__version__ = get_distribution("google-cloud-accessor").version

from gcp_accessor.gcs_accessor import GoogleComputeStorageAccessor

__all__ = ["__version__", "GoogleComputeStorageAccessor"]
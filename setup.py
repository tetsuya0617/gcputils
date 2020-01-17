from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="gcp-accessor",
    version="0.0.6",
    description="Accessor to Google Cloud Storage and Big Query",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(exclude=[".gitignore", "tests"]),
    url="https://github.com/tetsuya0617/gcputils",
    author="Tetsuya Hirata",
    author_email="tetsuya.hirata@classi.jp",
    maintainer="Tetsuya Hirata",
    maintainer_email="tetsuya.hirata@classi.jp",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
        "Topic :: Utilities",
    ],
    install_requires=["google-cloud-storage", "google-cloud-bigquery"],
)

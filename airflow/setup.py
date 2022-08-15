import setuptools

setuptools.setup(
    name="emr_serverless",
    version='1.0.0',
    packages= setuptools.PEP420PackageFinder.find(include=['emr_serverless', 'emr_serverless.*'], exclude=['dags.*', 'artifacts.*']),
    install_requires=[
        'boto3~=1.23.9',
        'botocore~=1.26.9',
    ],
)
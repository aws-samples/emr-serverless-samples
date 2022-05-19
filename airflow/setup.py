import setuptools

setuptools.setup(
    name="emr_serverless",
    packages= setuptools.PEP420PackageFinder.find(include=['emr_serverless', 'emr_serverless.*'], exclude=['dags.*', 'artifacts.*']),
)
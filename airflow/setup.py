import setuptools

setuptools.setup(
    name="emr_serverless",
    version='1.0.1',
    packages= setuptools.PEP420PackageFinder.find(include=['emr_serverless', 'emr_serverless.*'], exclude=['dags.*', 'artifacts.*']),
    install_requires=[
        'boto3>=1.23.9,~=1.23'
    ],
)
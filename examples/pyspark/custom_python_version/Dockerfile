ARG PYTHON_VERSION=3.10.6

FROM --platform=linux/amd64 amazonlinux:2 AS base
ARG PYTHON_VERSION

# Install Python 3.10.6 - Note that python 3.10 requires OpenSSL >= 1.1.1
RUN yum install -y gcc openssl11-devel bzip2-devel libffi-devel tar gzip wget make && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --enable-optimizations && \
    make install
    
# Create our virtual environment
# we need both --copies for python executables for cp for libraries
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV --copies
RUN cp -r /usr/local/lib/python3.10/* $VIRTUAL_ENV/lib/python3.10/

# Ensure our python3 executable references the virtual environment
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Upgrade pip (good practice) and install venv-pack
# You can install additional packages here or copy requirements.txt
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install venv-pack==0.2.0

# Package the env
# note you have to supply --python-prefix option to make sure python starts with the path where your copied libraries are present
RUN mkdir /output && \
    venv-pack -o /output/pyspark_${PYTHON_VERSION}.tar.gz --python-prefix /home/hadoop/environment

FROM scratch AS export
ARG PYTHON_VERSION
COPY --from=base /output/pyspark_${PYTHON_VERSION}.tar.gz /
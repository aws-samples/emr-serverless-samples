# This will pip install glow and build a virtualenv tar.gz
FROM python:3.7.9-buster AS python_build

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install \
    glow.py==1.1.2 \
    venv-pack==0.2.0

RUN mkdir /output && venv-pack -o /output/pyspark_glow.tar.gz


# This will clone the same version of glow and build an uberjar
FROM mozilla/sbt:8u292_1.5.7 AS jar_build

# We need conda for Glow build
RUN curl https://repo.anaconda.com/pkgs/misc/gpgkeys/anaconda.asc | gpg --dearmor > conda.gpg && \
    install -o root -g root -m 644 conda.gpg /usr/share/keyrings/conda-archive-keyring.gpg && \
    gpg --keyring /usr/share/keyrings/conda-archive-keyring.gpg --no-default-keyring --fingerprint 34161F5BF5EB1D4BFBBB8F0A8AEB4F8B29D82806 && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/conda-archive-keyring.gpg] https://repo.anaconda.com/pkgs/misc/debrepo/conda stable main" > /etc/apt/sources.list.d/conda.list

RUN apt update && \
    apt install -y \
    conda

# Set up conda and change the shell to bash
RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc
SHELL ["/bin/bash", "-c"]

WORKDIR /build

# Normally I would just download the release archive, but sbt core/assembly requires the git info
RUN git clone https://github.com/projectglow/glow.git && \
    cd glow && \
    git checkout v1.1.2

WORKDIR /build/glow

RUN source ~/.bashrc && conda env create -f python/environment.yml && \
    conda activate glow && \
    sbt core/assembly


# Finally, copy both the main artifacts to a scratch image that we can export
FROM scratch AS export
COPY --from=python_build /output/pyspark_glow.tar.gz /
COPY --from=jar_build /build/glow/core/target/scala-2.12/glow-spark3-assembly-1.1.2-SNAPSHOT.jar /
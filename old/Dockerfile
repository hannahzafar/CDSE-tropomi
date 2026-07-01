FROM continuumio/miniconda3

COPY . /app

WORKDIR /app

RUN conda env create -f environment.yml -n tropomi-test-env

RUN conda init bash && conda activate tropomi-test-env






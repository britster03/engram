ARG BASE_IMAGE=engram-lifecycle-native:20260930
FROM ${BASE_IMAGE} AS benchmark
USER root
RUN pip install nltk==3.9.1 rouge-score==0.1.2 pytest==8.3.3
ENV PYTHONPATH=/workspace PYTHONDONTWRITEBYTECODE=1
WORKDIR /workspace
FROM benchmark AS scoring
RUN pip install pandas==2.2.3 matplotlib==3.9.2
RUN pip install --target /opt/lifecycle-scoring --no-deps \
    bert-score==0.3.13 transformers==4.44.2 tokenizers==0.19.1 \
    huggingface-hub==0.24.7 safetensors==0.4.5
ENV PYTHONPATH=/opt/lifecycle-scoring:/workspace

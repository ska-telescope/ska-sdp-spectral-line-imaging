FROM python:3.10-slim as BUILD
WORKDIR /build
COPY . ./
RUN pip install --no-cache-dir poetry==1.8.3 && \
    poetry export --all-extras --without-hashes -o requirements.txt && \
    poetry build


FROM python:3.10-slim

RUN apt-get update -y && apt-get upgrade -y && \
apt-get clean && \
rm -rf /var/lib/apt/lists

RUN pip install --no-cache-dir pip==23.3

WORKDIR /install

COPY --from=BUILD /build/dist/*.whl /build/requirements.txt ./
RUN pip install --no-cache-dir --no-compile -r requirements.txt ska_sdp_spectral_line_imaging*.whl && \
  pip install --no-cache-dir jupyterlab==4.2.5 && \
  apt-get remove -y --autoremove libsqlite3-0 libgssapi-krb5-2 libk5crypto3 libkrb5support0 && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=BUILD /build/examples/install-config-and-run-pipeline.sh ./

ENTRYPOINT ["spectral-line-imaging-pipeline"]

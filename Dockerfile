FROM python:3.10-slim as BUILD
WORKDIR /build
COPY . ./
RUN pip install --no-cache-dir poetry==1.8.3 && \
    poetry export --without-hashes -o requirements.txt && \
    poetry build


FROM python:3.10-slim

RUN apt-get update -y && apt-get upgrade -y && \
  pip install --no-cache-dir pip==23.3
WORKDIR /install

COPY --from=BUILD /build/dist/*.whl /build/requirements.txt ./
RUN pip install --no-cache-dir --no-compile -r requirements.txt ska_sdp_spectral_line_imaging*.whl && \
  pip uninstall -y pip jupyterlab && \
  apt-get remove -y libsqlite3-0 libgssapi-krb5-2 libk5crypto3 libkrb5support0 && \
  apt-get remove -y --allow-remove-essential --autoremove perl-base && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENTRYPOINT ["spectral-line-imaging-pipeline"]

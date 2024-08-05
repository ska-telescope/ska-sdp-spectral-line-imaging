FROM python:3.10-slim as build
WORKDIR /build
COPY . ./
RUN pip install --no-cache-dir poetry==1.8.3 && \
    poetry export --without-hashes -o requirements.txt && \
    poetry build


FROM python:3.10-slim

WORKDIR /install

COPY --from=build /build/dist/*.whl /build/requirements.txt ./
RUN pip install --no-cache-dir --no-compile -r requirements.txt ska_sdp_spectral_line_imaging*.whl

WORKDIR /app

ENTRYPOINT ["spectral-line-imaging-pipeline"]

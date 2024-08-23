Bootstrap: docker
From: python:3.10-slim
Stage: build

%post
mkdir -p /build

%files
./ /build

%post
cd /build && \
pip install --no-cache-dir poetry==1.8.3 && \
poetry export --without-hashes -o requirements.txt && \
poetry build && \
mkdir -p /install && \
cp  /build/dist/*.whl /install && \
cp  /build/requirements.txt /install

cd /install
pip install --no-cache-dir --no-compile -r requirements.txt ska_sdp_spectral_line_imaging*.whl
rm -rf /build

mkdir -p /app
cd /app
rm -rf /install

%runscript
cd /app
exec spectral-line-imaging-pipeline "$@"
%startscript
cd /app
exec spectral-line-imaging-pipeline "$@"

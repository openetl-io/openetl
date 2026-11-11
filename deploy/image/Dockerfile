# openetld's image: openetld, openetl-engine, the official connectors, the
# openetl CLI and the console, built for Linux into the context beforehand
# (make image). Every role runs it, as do pod launchers' pods; Ubuntu for
# the engine's glibc. The server serves the console with
# OPENETL_WEB=/opt/openetl/web.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/* \
	&& mkdir -p /var/lib/openetl && chown 65532:65532 /var/lib/openetl
COPY openetld openetl-engine openetl /opt/openetl/
COPY connectors/ /opt/openetl/connectors/
COPY web/ /opt/openetl/web/
# A release's image carries THIRD-PARTY-NOTICES.txt too.
COPY LICENSE NOTICE THIRD-PARTY-NOTICES* /opt/openetl/
ENV PATH=/opt/openetl:$PATH
USER 65532:65532
WORKDIR /var/lib/openetl
ENTRYPOINT ["openetld"]

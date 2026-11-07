# openetld's image: openetld, openetl-engine and the official connectors,
# built for Linux into the context beforehand (make image). Runners, pod
# launchers and their pods run it; Ubuntu for the engine's glibc.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/* \
	&& mkdir -p /var/lib/openetl && chown 65532:65532 /var/lib/openetl
COPY openetld openetl-engine /opt/openetl/
COPY connectors/ /opt/openetl/connectors/
ENV PATH=/opt/openetl:$PATH
USER 65532:65532
WORKDIR /var/lib/openetl
ENTRYPOINT ["openetld"]

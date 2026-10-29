FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y libpq-dev gcc postgresql-client

#================================================================
# pip install required modules
#================================================================

RUN pip install --upgrade setuptools
ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

#==================================================
# Add the latest code
#==================================================

RUN mkdir -p /pgevents
WORKDIR /pgevents

ADD . /pgevents

# Make health check script executable
RUN chmod +x /pgevents/producer/health_check.sh

RUN pip install -e .

# CVE-2026-111497: upgrade inherited Brotli packages to the distro security fixes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade libbrotli1 libbrotli-dev \
    && . /etc/os-release \
    && case "$VERSION_CODENAME" in \
        bookworm) brotli_fixed_version='1.0.9-2+deb12u1' ;; \
        trixie) brotli_fixed_version='1.1.0-2+deb13u1' ;; \
        *) echo "Unsupported Debian release: $VERSION_CODENAME" >&2; exit 1 ;; \
    esac \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' libbrotli1)" ge "$brotli_fixed_version" \
    && rm -rf /var/lib/apt/lists/*

CMD ["/bin/bash"]

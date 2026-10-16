FROM mcr.microsoft.com/devcontainers/java:1-17-bookworm

# The base image preconfigures a dl.yarnpkg.com apt source with an expired/missing
# GPG key, which makes `apt-get update` fail outright; we don't need Yarn here.
RUN rm -f /etc/apt/sources.list.d/yarn.list

# Installed directly via apt instead of the devcontainer "python" feature,
# which was failing with exit code 100 (apt-get error) during feature install.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/local/bin/python

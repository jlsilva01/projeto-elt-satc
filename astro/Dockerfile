FROM astrocrpublic.azurecr.io/runtime:3.0-2

USER root

RUN apt-get update && \
    apt-get install -y curl gnupg2 apt-transport-https ca-certificates unixodbc unixodbc-dev gcc g++ && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER astro

FROM goatcommunity/data-preparation-base:0.03

# Install GTFSTIDY 
RUN bash -c "cd /home && wget https://go.dev/dl/go1.19.2.linux-amd64.tar.gz && rm -rf /usr/local/go && tar -C /usr/local -xzf go1.19.2.linux-amd64.tar.gz"
ENV PATH="${PATH}:/usr/local/go/bin" 
ENV GOPATH="/root/go"
ENV PATH="${GOPATH}/bin:${PATH}"
RUN bash -c "go install github.com/patrickbr/gtfstidy@latest"

# Upgrade node to latest version (TODO: remove this when the base image is updated)
RUN apt-get update
RUN echo "y" | apt-get install curl
# nvm env vars
RUN mkdir -p /usr/local/nvm
ENV NVM_DIR /usr/local/nvm
# IMPORTANT: set the exact version
ENV NODE_VERSION v16.17.0
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
RUN /bin/bash -c "source $NVM_DIR/nvm.sh && nvm install $NODE_VERSION && nvm use --delete-prefix $NODE_VERSION"
# add node and npm to the PATH
ENV NODE_PATH $NVM_DIR/versions/node/$NODE_VERSION/bin
ENV PATH $NODE_PATH:$PATH
RUN npm -v
RUN node -v

# Install GTFS-VIA-POSTGRES
RUN bash -c "cd /home && apt-get update && apt install npm -y && git clone https://github.com/majkshkurti/gtfs-via-postgres.git && cd gtfs-via-postgres && npm install --production && npm cache clean --force && ln -s /home/gtfs-via-postgres/cli.js /usr/local/bin/gtfs-via-postgres"

# Copy poetry.lock* in case it doesn't exist in the repo
COPY ./pyproject.toml ./poetry.lock* /app/
ENV PYTHONPATH "${PYTHONPATH}:."
WORKDIR /app/

# Allow installing dev dependencies to run tests
ARG INSTALL_DEV=false
RUN bash -c "if [ $INSTALL_DEV == 'true' ] ; then poetry install --no-root ; else poetry install --no-root --no-dev ; fi"

# Install aws cli
WORKDIR /tmp/
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# Install s3fs
RUN apt update
RUN apt install -y s3fs

# Install kart 
RUN apt-get install -y libtinfo5
RUN wget "https://goat-db-schemas.s3.eu-central-1.amazonaws.com/kart.deb"
RUN dpkg -i kart.deb

# install posgresql-client
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update
RUN apt-get install -y postgresql-client-14

WORKDIR /app/

ENTRYPOINT ["tail"]
CMD ["-f","/dev/null"]


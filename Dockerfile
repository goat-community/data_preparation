FROM goatcommunity/data-preparation-base:0.02

# Install libpostal
WORKDIR /tmp/
RUN apt-get install -y git curl autoconf automake libtool python-dev pkg-config
RUN git clone https://github.com/openvenues/libpostal
WORKDIR /tmp/libpostal/
RUN ./bootstrap.sh
RUN ./configure
RUN make
RUN make install

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
RUN wget "https://github.com/koordinates/kart/releases/download/v0.11.5/kart_0.11.5-1_amd64.deb"
RUN dpkg -i kart_0.11.5-1_amd64.deb

WORKDIR /app/

ENTRYPOINT ["tail"]
CMD ["-f","/dev/null"]


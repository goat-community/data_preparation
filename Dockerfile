FROM goatcommunity/data-preparation-base:0.02

# Copy poetry.lock* in case it doesn't exist in the repo
COPY ./pyproject.toml ./poetry.lock* /app/
ENV PYTHONPATH "${PYTHONPATH}:."
WORKDIR /app

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

WORKDIR /app/

ENTRYPOINT ["tail"]
CMD ["-f","/dev/null"]


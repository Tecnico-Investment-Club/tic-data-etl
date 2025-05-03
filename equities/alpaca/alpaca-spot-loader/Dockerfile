FROM python:3.11.3-alpine3.18

LABEL maintainer="Tomas Juhos"
USER root

WORKDIR /project

COPY requirements.txt .

# install dependencies
RUN apk update \
    && python -m pip install --no-cache-dir --upgrade pip \
    && apk add --no-cache \
        postgresql-dev \
        gcc \
        libc-dev \
        make \
        dumb-init \
    && pip3 install --no-cache-dir -r requirements.txt \
    && rm -rf /root/./.cache \
    && find . -type d -name __pycache__ -exec rm -r {} \+ \
    && find . -type d -name .pyc -exec rm -r {} \+

COPY src/ .
COPY pyproject.toml .
COPY README.md .
RUN pip3 install .
EXPOSE 9000

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["python", "-m", "alpaca_spot_loader"]
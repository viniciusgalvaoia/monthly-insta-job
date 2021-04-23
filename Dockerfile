FROM python:3.9-slim

# Set environment variables
ENV PIP_TRUSTED_HOST pypi.pd-internal.com
ENV PIP_INDEX_URL http://pypi.pd-internal.com
WORKDIR workspace

# Install base requirements
ADD requirements/* requirements/
RUN pip install -r requirements/run.txt

# https://github.com/pyupio/safety
RUN pip install safety

# Set arguments
ARG vault="http://dev-vault-api.pd-internal.com"
ARG env="dev"
ARG full_env="development"
ARG job_name

# Set environment variables
ENV PD_PYTHON_ENV $full_env
ENV PD_VAULT_API_URL $vault

# Copy the job folder
ADD . .

CMD python -m job

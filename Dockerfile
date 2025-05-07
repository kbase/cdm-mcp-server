FROM ghcr.io/kbase/cdm-spark-standalone:pr-29

# Switch to root to install packages
USER root

RUN apt-get update && apt-get install -y \
    # required for psycopg
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars

ENV CONFIG_DIR=/opt/config
COPY ./config/ ${CONFIG_DIR}
ENV SPARK_FAIR_SCHEDULER_CONFIG=${CONFIG_DIR}/spark-fairscheduler.xml

COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

WORKDIR /app

# Copy the application code into the container
COPY ./src /app/src

COPY ./scripts/ /opt/scripts/
RUN chmod a+x /opt/scripts/*.sh

# Switch back to non-root user
USER spark_user

ENTRYPOINT ["/opt/scripts/mcp-server-entrypoint.sh"]
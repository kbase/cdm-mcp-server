FROM --platform=linux/amd64 ghcr.io/kbase/cdm-spark-standalone:pr-28

# Switch to root to install packages
USER root

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

CMD ["python", "-m", "src.main"] 
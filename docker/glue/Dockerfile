FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

USER root

# Install dependencies into Glue python environment
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

# Ensure Spark event log directory exists with write permissions for glue_user
RUN mkdir -p /tmp/spark-events && chmod 777 /tmp/spark-events

# Copy entrypoint startup script with executable permissions
COPY entrypoint.sh /home/glue_user/entrypoint.sh
RUN chmod 755 /home/glue_user/entrypoint.sh && \
    chown glue_user /home/glue_user/entrypoint.sh || true

# Switch back to non-root glue_user
USER glue_user

WORKDIR /home/glue_user/workspace

# Default entrypoint launches Spark History Server, Livy, and JupyterLab
CMD ["/home/glue_user/entrypoint.sh"]

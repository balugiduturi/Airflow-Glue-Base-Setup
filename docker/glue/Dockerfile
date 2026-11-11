FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

USER root

# Install dependencies into Glue python environment
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

# Switch back to non-root glue_user
USER glue_user

WORKDIR /home/glue_user/workspace

# Default command starts Jupyter server or allows interactive execution
CMD ["/home/glue_user/jupyter/jupyter_start.sh"]


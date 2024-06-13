FROM registry.access.redhat.com/ubi9/python-311:1-62.1717085982

WORKDIR /app

COPY --chown=1001:0 *.py requirements.txt ./

USER 1001

RUN pip install --no-cache-dir -r requirements.txt

CMD sleep 60

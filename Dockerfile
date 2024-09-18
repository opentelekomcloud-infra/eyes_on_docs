FROM registry.access.redhat.com/ubi9/python-311:1-72.1724040033

WORKDIR /app

COPY --chown=1001:0 requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=1001:0 $(find . -name "*.py") ./

USER 1001

CMD sleep 60

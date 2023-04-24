FROM registry.access.redhat.com/ubi9/python-39:1-114.1681379027

WORKDIR /app

COPY --chown=1001:0 gitea_info.py github_info.py open_issues.py requirements.txt ./
USER 1001

RUN pip install --no-cache-dir -r requirements.txt

CMD sleep 60
FROM apache/superset:3.0.0-py310

USER root 

COPY wait-for-it.sh /wait-for-it.sh
COPY docker-entrypoint.sh /docker-entrypoint.sh

RUN chmod +x /wait-for-it.sh /docker-entrypoint.sh

User superset

ENTRYPOINT ["/docker-entrypoint.sh"]


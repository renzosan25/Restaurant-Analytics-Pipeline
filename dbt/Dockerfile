FROM ghcr.io/dbt-labs/dbt-postgres:1.9.latest

WORKDIR /usr/app


COPY my_project/ /usr/app/
COPY profiles.yml /root/.dbt/profiles.yml
COPY wait-for-it.sh /wait-for-it.sh
COPY docker-entrypoint.sh /docker-entrypoint.sh


RUN chmod +x /wait-for-it.sh /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["dbt", "run"]


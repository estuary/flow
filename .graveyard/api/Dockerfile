FROM postgrest/postgrest as postgrest
FROM nginx

COPY --from=postgrest /bin/postgrest /bin/postgrest
COPY nginx.conf /etc/nginx/nginx.conf
COPY postgrest.conf /etc/postgrest.conf
COPY entrypoint.sh /bin/entrypoint.sh

# DATABASE_URL is intended to be replaced with an injected secret.
ENV DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres

# Default PORT, which can be overridden.
ENV PORT=8080
EXPOSE 8080

CMD /bin/entrypoint.sh
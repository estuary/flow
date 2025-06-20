FROM denoland/deno

EXPOSE 8080

WORKDIR /app

ADD . /app

RUN deno install --entrypoint index.ts

CMD ["run", "--allow-net", "index.ts"]

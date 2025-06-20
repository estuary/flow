FROM denoland/deno

EXPOSE 8000

WORKDIR /app

ADD . /app

RUN deno install --entrypoint index.ts

CMD ["run", "--allow-env", "--allow-net", "index.ts"]

# Control Plane

The Control Plane orchestrates actions taken by API users against the Data Plane.

## Local Development

To run on the local machine, you can just use Cargo from the `control` directory.

```bash
$ cargo test
$ cargo run
```

## Docker

The `control` crate is part of the larger Flow workspace. This means we must use the Flow workspace root as the Docker context to access the `Cargo.lock` file.

```bash
$ cd ../.. # to flow's root dir
$ docker build . --file control.Dockerfile -t control:dev
$ docker run --rm -it -p 3000:3000 control:dev
```

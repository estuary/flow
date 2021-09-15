# flowctl.sh

This script is a Docker wrapper around the flowctl multi-call binary. This allows you to do development
on your local machine without installing all the dependancies of Flow.

# Requirements
In order to use this script you must have Docker installed on your machine. By default it launches
Flow on the host network which shares the same network as your machine. By default Flow works on port 8080
so if you have something else on that port you may need to change Flow's port. See the `--help` options for more
information on those config parameters.

## Installation
To install this script, you need to put it somewhere that is in your path. For /usr/local/bin:
```bash
curl -OL https://raw.githubusercontent.com/estuary/flow/master/scripts/flowctl.sh
chmod 755 flowctl.sh
sudo mv flowctl.sh /usr/local/bin/flowctl.sh
sudo ln -s /usr/local/bin/flowctl.sh /usr/local/bin/flowctl
```
This also creates a symlinks for `flowctl` to the script.

## Running
You should be able to use the command `flowctl` just as in the documentation. (See Caveats)

## Caveats

 * Because this script runs Flow in Docker, you must take care with file paths as they may not work inside of the container.
The script automatically detects where you are referencing your source file and data directories and maps those inside of the container.
If your config references something outside of that directory, it may not be inside of the container and could be lost.

For example, if you have an sqlite database path set to `../example.db` it will likely generate that database outside of the
mapped directory in the container.

* On MacOS it must run the container as root. This is due to how Docker for Mac manages permissions on the docker.sock file. Despite
this, all files created by the container on the user's filesystem will be owned by the user (rather than root)

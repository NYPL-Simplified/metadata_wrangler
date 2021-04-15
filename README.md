# Library Simplified Metadata Wrangler
[![Build Status](https://api.travis-ci.com/NYPL-Simplified/metadata_wrangler.svg?branch=master)](https://travis-ci.com/github/NYPL-Simplified/metadata_wrangler)

This is the Metadata Wrangler for [Library Simplified](https://librarysimplified.org/). The Metadata Wrangler server utilizes and intelligently amalgamates a wide variety of information sources for library ebooks and incorporates them into the reading experience for users by improving selection, search, and recommendations.

This application depends on [Library Simplified Server Core](https://github.com/NYPL-Simplified/server_core) as a git submodule.

## Local Installation

Thorough deployment instructions, including essential libraries for Linux systems, can be found [in the Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions). **_If this is your first time installing a Library Simplified server, please review those instructions._**

Keep in mind that the Metadata Wrangler server requires unique database names and a data directory, as detailed below.

Once the database is running, run the application locally with `python app.py` and go to `http://localhost:7000`. If you rather run this server locally through Docker, read the "Docker" section below, though this option doesn't currently allow for local development.

### Database

When installing and running a local Metadata Wrangler, you need to create the relevant databases in Postgres. If you are using Docker, you can skip this step since the Postgres database will be created in a container.

```sh
$ sudo -u postgres psql
CREATE DATABASE simplified_metadata_dev;
CREATE DATABASE simplified_metadata_test;

# Create users, unless you've already created them for another Library Simplified project
CREATE USER simplified with password '[password]';
CREATE USER simplified_test with password '[password]';

grant all privileges on database simplified_metadata_dev to simplified;
grant all privileges on database simplified_metadata_test to simplified_test;
```

### Data Directory

Clone the Library Simplified data directory to a location of your choice:
```sh
$ git clone https://github.com/NYPL-Simplified/data.git YOUR_DATA_DIRECTORY
```

In your content server configuration file, your specified "data_directory" should be YOUR_DATA_DIRECTORY.

## Testing

The Github Actions CI service runs the pytest unit tests against Python 3.6, 3.7, 3.8 and 3.9 automatically using [tox](https://tox.readthedocs.io/en/latest/).

To run `pytest` unit tests locally, install `tox`. Make sure you're in the current virtual environment. 

```
$ pip install tox
```

Then run `tox` to run the pytests in all Python versions. 

```
$ tox
```

This uses the local Postgres database by default so that service should be running. If you rather depend on using Docker to spin up a Postgres container for testing, read more in the "Testing with Docker" section below.

Tox has an environment for each python version and an optional `-docker` factor that will automatically use docker to deploy service containers used for the tests. You can select the environment you would like to test with the tox `-e` flag. More on this in the following sections.

### Environments

| Environment | Python Version |
| ----------- | -------------- |
| py36        | Python 3.6     |
| py37        | Python 3.7     | 
| py38        | Python 3.8     | 
| py39        | Python 3.9     |

All of these environments are tested by default when running tox. To test one specific environment you can use the `-e` flag.

To run pytest only with Python 3.8, for example, run:

```
tox -e py38
```

You need to have the Python versions you are testing against installed on your local system. `tox` searches the system for installed Python versions, but does not install new Python versions. If `tox` doesn't find the Python version its looking for it will give an `InterpreterNotFound` errror.

[Pyenv](https://github.com/pyenv/pyenv) is a useful tool to install multiple Python versions, if you need to install missing Python versions in your system for local testing.

### Testing with Docker

If you install `tox-docker`, `tox` will take care of setting up all the service containers necessary to run the unit tests and pass the correct environment variables to configure the tests to use these services. Using `tox-docker` is not required, but it is the _recommended_ way to run the tests locally, since it runs the tests in the same way they are run on the Github Actions CI server. 

```sh
$ pip install tox-docker
``` 

The docker functionality is included in a `docker` factor that can be added to the environment. To run an environment
with a particular factor you add it to the end of the environment. 

To test with Python 3.8 using docker containers for the services, run:

```sh
$ tox -e py38-docker
```

### Local services

If you already have Postgres running locally, you can use that service instead by setting the following environment variable:

- `SIMPLIFIED_TEST_DATABASE`

Make sure the ports and usernames are updated to reflect the local configuration.

```sh
# Set environment variables
$ export SIMPLIFIED_TEST_DATABASE="postgres://simplified_test:test@localhost:9005/simplified_metadata_test"

# Run tox
$ tox -e py38
```

### Override `pytest` arguments

If you wish to pass additional arguments to `pytest` you can do so through `tox`. The default argument passed to `pytest` is `tests`, however you can override this. Every argument passed after a `--` to the `tox` command will then be passed to `pytest`, overriding the default.

For example, when you only want to test changes in one test file, you can pass the path to the file after `--`. To run the `test_content_cafe.py` tests with Python 3.6 using docker, run:

```sh
$ tox -e py36-docker -- tests/test_content_cafe.py
```

To run specific tests within a file, pass in the test class and the optional function name in the following format:

```sh
$ tox -e py36-docker -- tests/test_content_cafe.py::TestContentCafeAPI::test_from_config
```

## Docker

Docker is used to run the application server, a scripts server, and a database in containers that communicate with each other. This allows for easy deployment but can't currently be used for local development. This is because the current installation script installs the repo by cloning it from Github, and not the current local file system.

In the `/docker` directory, there are three `Dockerfile`s for each separate container service. Rather than running each container individually, use the `./docker-compose.yml` file and the `docker compose` command to orchestrate building and running the containers.

_Note: The `docker compose` command is "experimental" but will become the default command to use `docker-compose.yml` files. The existing command line tool `docker-compose` is still supported if that tool is preferred. Just replace the following `docker compose` commands with `docker-compose`._

### Running the Containers

To build and start the three containers, run:

```sh
$ docker compose up -d
```

Once the base images are downloaded, the server images are built, and the servers are running, visit `http://localhost` for the Metadata Wrangler homepage. The `-d` flag runs the command in "detached" mode so they will run in the background. If you need to rebuild the images, add the `--build` flag.

It's possible to stop running the containers but not to remove them using:

```sh
$ docker compose stop
```

You can re-start the existing containers with:

```sh
$ docker compose start
```

If you want to stop _and_ remove the containers, run:

```sh
$ docker compose down
```

### Debugging Local Containers

It's possible to get access to a container to see local files and logs. First, find the container ID of the service you want to get access to. The following command will list all containers, running and stopped, on the machine:

```sh
$ docker ps --all
```

This will return a list of containers and the relevant containers created by this repo's `docker-compose.yml` file will be "metadata_wrangler_scripts", "metadata_wrangler_webapp", and "postgres12.0-alpine". Once you get the "Container ID" of the service you want access to, for example `56dcb9e3da3b`, run:

```sh
$ docker exec -it 56dcb9e3da3b bash
```

This will give you bash access to the container to find logs, located at the given directory for each container's `volumes` directory configuration in `docker-compose.yml`. To exit, run `exit`.

## License

```
Copyright © 2015 The New York Public Library, Astor, Lenox, and Tilden Foundations

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

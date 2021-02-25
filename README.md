# Library Simplified Metadata Wrangler
[![Build Status](https://api.travis-ci.com/NYPL-Simplified/metadata_wrangler.svg?branch=master)](https://travis-ci.com/github/NYPL-Simplified/metadata_wrangler)

This is the Metadata Wrangler for [Library Simplified](https://librarysimplified.org/). The metadata server utilizes and intelligently amalgamates a wide variety of information sources for library ebooks and incorporates them into the reading experience for users by improving selection, search, and recommendations.

It depends on the [LS Server Core](https://github.com/NYPL-Simplified/server_core) as a git submodule.

## Installation

Thorough deployment instructions, including essential libraries for Linux systems, can be found [in the Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions). **_If this is your first time installing a Library Simplified server, please review those instructions._**

Keep in mind that the metadata server requires unique database names and a data directory, as detailed below.

### Database

Create relevant databases in Postgres:
```sh
$ sudo -u postgres psql
CREATE DATABASE simplified_metadata_test;
CREATE DATABASE simplified_metadata_dev;

# Create users, unless you've already created them for another LS project
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

# Library Simplified Metadata Wrangler

This is the Metadata Wrangler for [Library Simplified](http://www.librarysimplified.org/). The metadata server utilizes and intelligently almagamates a wide variety of information sources for library ebooks and incorporates them into the reading experience for users by improving selection, search, and recommendations.

It depends on the [LS Server Core](https://github.com/NYPL/Simplified-server-core) as a git submodule.

## Installation

Thorough deployment instructions, including essential libraries for Linux systems, can be found [in the Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified-iOS/wiki/Deployment-Instructions). **_If this is your first time installing a Library Simplified server, please review those instructions._**

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
$ git clone https://github.com/NYPL/Simplified-data.git YOUR_DATA_DIRECTORY
```

In your content server configuration file, your specified "data_directory" should be YOUR_DATA_DIRECTORY.

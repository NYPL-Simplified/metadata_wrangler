#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

repo="$1"
version="$2"

apt-get update && $minimal_apt_get_install \
  software-properties-common \
  python3.6 \
  python3-dev \
  python3-wheel \
  python3-setuptools \
  python3-venv \
  python3-pip \
  python-cairo \
  python-nose \
  gcc \
  git \
  libpcre3 \
  libpcre3-dev \
  libffi-dev \
  libjpeg-dev \

# Create a user.
useradd -ms /bin/bash -U simplified

# Get the proper version of the codebase.
mkdir /var/www && cd /var/www
git clone https://github.com/${repo}.git metadata
chown simplified:simplified metadata 
cd metadata
git checkout $version

# Use https to access submodules.
git config submodule.core.url https://github.com/NYPL-Simplified/server_core.git
git submodule update --init --recursive

# Add a .version file to the directory. This file
# supplies an endpoint to check the app's current version.
printf "$(git describe --tags)" > .version

python3 -m venv env

# Pass runtime environment variables to the app at runtime.
touch environment.sh
SIMPLIFIED_ENVIRONMENT=/var/www/metadata/environment.sh
echo "if [[ -f $SIMPLIFIED_ENVIRONMENT ]]; then \
      source $SIMPLIFIED_ENVIRONMENT; fi" >> env/bin/activate

# Install required python libraries.
set +x && source env/bin/activate && set -x

# Update pip and setuptools.
python3 -m pip install -U pip setuptools
# Install the necessary dev requirements.
python3 -m pip install -r requirements-dev.txt

# Install NLTK.
python3 -m textblob.download_corpora
mv /root/nltk_data /usr/lib/

# Link the repository code to /home/simplified and change permissions
su - simplified -c "ln -s /var/www/metadata /home/simplified/metadata"
chown -RHh simplified:simplified /home/simplified/metadata

# Give logs a place to go.
mkdir /var/log/simplified

# Copy scripts that run at startup.
cp /ls_build/startup/* /etc/my_init.d/

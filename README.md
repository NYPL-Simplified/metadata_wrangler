= Library Simplified Server

== Install system packages

=== On Debian

sudo apt-get install python-virtualenv python-dev libpq-dev libxml2-dev libxslt-dev 

=== On EC2 AMI

First, we have to install Python 2.7. These instructions adapted from
http://www.lecloud.net/post/61401763496/install-update-to-python-2-7-and-latest-pip-on-ec2

# install build tools 
sudo yum install make automake gcc gcc-c++ kernel-devel git-core -y

# install python 2.7 and change default python symlink 
sudo yum install python27-devel -y
sudo rm /usr/bin/python
sudo ln -s /usr/bin/python2.7 /usr/bin/python 

# yum still needs 2.6, so write it in and backup script 
sudo cp /usr/bin/yum /usr/bin/_yum_before_27
sudo sed -i s/python/python2.6/g /usr/bin/yum
sudo sed -i s/python2.6/python2.6/g /usr/bin/yum 

# should display now 2.7.5 or later:
python -V

# now install pip for 2.7
# Download https://bootstrap.pypa.io/ez_setup.py and verify its contents.
sudo python ez_setup.py
sudo /usr/bin/easy_install-2.7 pip
sudo pip install virtualenv

# These are tools necessary for Simplified itself.
sudo yum install git postgresql python-pip gcc python-devel postgresql-devel libxml2-devel libxslt-devel

== Create virtual environment

cd ~/
mkdir ./.virtualenv
cd ./.virtualenv
virtualenv default
source ~/.virtualenv/default/bin/activate

Optional: Add the above 'source' command to .bashrc

== Install Python packages through pip

pip install virtualenvwrapper
pip install Flask
pip install nose
pip install psycopg2
pip install pyatom
pip install beautifulsoup4
pip install requests
pip install rdflib
pip install gunicorn
pip install isbnlib
pip install pillow
pip install sqlalchemy
pip install lxml

# Should only be used by metadata wrangler
pip install textblob
pip install pyld
pip install tinys3
pip install numpy
pip install scipy scikit-learn
# blas, liblapack

== Add secrets

Put API keys and the like in .virtualenv/default/bin/activate as
environment variables.

== Database setup

These instructions are specific to a NYPL IT-managed EC2 AMI instance.

Modify /etc/rc.d/init.d/postgresql93 to refer to /opt/media/:

PGDATA=/opt/media/pgsql${PGSUFFIX}/data
PGLOG=/opt/media/pgsql${PGSUFFIX}/pgstartup.log

Modify /opt/media/pgsql93/data/pg_hba.conf to support password-based
authentication over local IP connections:

host    all             all             127.0.0.1/32		password
host    all             all             ::1/128                 password

Create a role for Simplified:

$ createuser simplified

Grant the 

alter role simplified with Superuser;
alter role simplified with Create DB;
alter user simplified password '[password]'

Change the DATABASE_URL and DATABASE_URL_TEST URLs to reflect the role
and password.

= Gutenberg Illustrated 

== Packaging

1. Install Processing.

2. Download the following files and unzip them into ~/sketchbook/libraries/

* controlP5 (http://www.sojamo.de/libraries/controlP5/)
* opencv_processing (https://github.com/atduskgreg/opencv-processing/releases)

3. Start Processing.

$ ./processing-java /path/to/imagecoverp5tint.pde

4. File > Export Application
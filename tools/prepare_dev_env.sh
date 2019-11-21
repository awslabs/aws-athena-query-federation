#!/bin/bash

# Copyright (C) 2019 Amazon Web Services
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cat << EOF
#
# This script will prepare your development environment by installing certain pre-requisites, namely:
#   1. Apache Maven
#   2. HomeBrew - a package manager that will be used to fetch the next two pre-requistes.
#   3. AWS CLI (latest version)
#   4. AWS SAM Build Tool (latest version)
#
# This script has been designed and tested to work on Amazon Linux but may require slight adjustment for other Operating Systems.
# All tools used here (except HomeBrew) are supported on all major Operating Systems: Windows, Linux, Mac OS.
#
# This script may prompt you for yes/no responses or permission to continue at verious points. It is not meant to run unattended.
#
EOF

while true; do
    read -p "Do you wish to proceed? (yes or no) " yn
    case $yn in
        [Yy]* ) echo "Proceeding..."; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

set -e
sudo wget https://archive.apache.org/dist/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz -O /tmp/apache-maven-3.5.4-bin.tar.gz
sudo tar xf /tmp/apache-maven-3.5.4-bin.tar.gz -C /opt
echo "export M2_HOME=/opt/apache-maven-3.5.4" >> ~/.profile
echo "export PATH=\${M2_HOME}/bin:\${PATH}" >> ~/.profile
echo "export M2_HOME=/opt/apache-maven-3.5.4" >> ~/.bash_profile
echo "export PATH=\${M2_HOME}/bin:\${PATH}" >> ~/.bash_profile

sudo yum -y install java-1.8.0-openjdk-devel
sudo update-alternatives --set java /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java
sudo update-alternatives --set javac /usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/javac

# If the above update-alternatives doesn't work and you don't know your path try
#  sudo update-alternatives --config java
#  sudo update-alternatives --config javac

sh -c "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install.sh)"
test -d ~/.linuxbrew && eval $(~/.linuxbrew/bin/brew shellenv)
test -d /home/linuxbrew/.linuxbrew && eval $(/home/linuxbrew/.linuxbrew/bin/brew shellenv)
test -r ~/.bash_profile && echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.bash_profile
echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.profile
echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.bash_profile

source ~/.profile

brew tap aws/tap
brew reinstall awscli
brew reinstall aws-sam-cli

aws --version
sam --version

echo ""
echo ""
echo "To ensure your terminal can see the new tools we installed run \"source ~/.profile\" or open a fresh terminal."
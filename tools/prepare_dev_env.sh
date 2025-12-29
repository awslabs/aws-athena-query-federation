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

# Choose Java version
echo ""
echo "Which Java version would you like to install?"
echo "1) Java 11"
echo "2) Java 17"
while true; do
    read -p "Enter your choice (1 or 2): " java_choice
    case $java_choice in
        1 ) JAVA_VERSION=11; break;;
        2 ) JAVA_VERSION=17; break;;
        * ) echo "Please enter 1 or 2.";;
    esac
done

set -e
sudo wget https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz -O /tmp/apache-maven-3.9.6-bin.tar.gz
sudo tar xf /tmp/apache-maven-3.9.6-bin.tar.gz -C /opt
echo "export M2_HOME=/opt/apache-maven-3.9.6" >> ~/.profile
echo "export PATH=\${M2_HOME}/bin:\${PATH}" >> ~/.profile
echo "export M2_HOME=/opt/apache-maven-3.9.6" >> ~/.bash_profile
echo "export PATH=\${M2_HOME}/bin:\${PATH}" >> ~/.bash_profile

# Install Java
sudo yum -y install java-${JAVA_VERSION}-openjdk-devel

# On Amazon Linux 2, if yum install fails, try:
#   sudo amazon-linux-extras install -y java-openjdk${JAVA_VERSION}
#
# On Amazon Linux 2023, use:
#   sudo dnf install java-${JAVA_VERSION}-amazon-corretto

echo "Set the default to the Java $JAVA_VERSION installation"
sudo update-alternatives --config java

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

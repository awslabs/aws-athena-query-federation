#!/bin/bash

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
sudo wget https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
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

source ~/.profile

brew tap aws/tap
brew reinstall awscli
brew reinstall aws-sam-cli
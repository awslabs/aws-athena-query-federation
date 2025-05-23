FROM ubuntu

RUN apt update -y && apt install -y tar gzip unzip wget less groff vim git python3 python3-pip unixodbc-dev curl jq

RUN pip3 install boto3

RUN curl -sL https://deb.nodesource.com/setup_18.x | bash -
RUN apt install -y aptitude # needed to resolve version conflicts from changing the source for node above
RUN apt install -y nodejs
RUN aptitude install -y npm

RUN wget https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -O /tmp/awscli-exe-linux-x86_64.zip && \
    unzip /tmp/awscli-exe-linux-x86_64.zip -d /tmp/awscli && \
    /tmp/awscli/aws/install


RUN wget https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-x86_64.zip -O /tmp/aws-sam-cli-linux-x86_64.zip && \
    unzip /tmp/aws-sam-cli-linux-x86_64.zip -d /tmp/sam-installation && \
    /tmp/sam-installation/install

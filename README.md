# Docker environment for Airflow 
<!-- GETTING STARTED -->

## Getting Started

Installation instructions and setup guidelines for running Airflow in Docker environment locally.

### Prerequisites
Download Docker Desktop and follow the instructions for <a href= "https://docs.docker.com/desktop/install/mac-install/">Mac</a>,
<a href= "https://docs.docker.com/desktop/install/linux-install/">Linux</a> and <a href= "https://docs.docker.com/desktop/install/windows-install/">Windows</a> operating systems

### Installation

Once you have Docker installed successfully:

* Clone the repo
   ```
   git clone git@github.com:MediaMath/bi-docker-env.git
   ```
* Build an Airflow image sourced from the Dockerfile:
   ```
   docker compose up --build
   ```

## Deploying Docker Containers on EC2:

Docker on amzn linux:

* Install docker with:
```
sudo yum install -y docker
```
* Start docker service:
```
sudo service docker start
```
* Download docker compose:
```
sudo  curl -SL https://github.com/docker/compose/releases/download/v2.18.1/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
```
* Apply executable permissions to docker compose:
```
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

* Clone the docker repo.
```
git clone git@github.com:MediaMath/bi-docker-env.git
```

Note: Run this before building docker container to avoid permission errors. Otherwise, docker will create the directories in the container under root. 
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```
* Add user to docker group:
```
 sudo gpasswd -a $USER docker

 newgrp docker 
```
* Build docker image:
```
docker compose up —build
```

* Create ssh key and add to git repo.
* Navigate to the ./dags directory and clone the bi repo:
```
git clone git@github.com:MediaMath/business-intelligence-infrastructure-python.git
```


Save your aws credentials in config and credentials files on ~/.aws directory on docker host. 

Note: ask admin to enable 8080 port on the host machine to access Airflow UI. Additionally, the ip address on the host machine need to be whitelisted through aws console to insure VPC peering with the Redshift server. 
   



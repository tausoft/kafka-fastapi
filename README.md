# kafka-fastapi
Docker file for ApacheKafka (Wurstmeister) &amp; FastAPI


<h1>Pre-Requisites</h1>
Install docker-compose https://docs.docker.com/compose/install/


<h1>Setup</h1>

Navigate to dir with terminal and clone the git


- `git clone https://github.com/tausoft/kafka-fastapi.git`

Start a cluster


- `cd kafka-fastapi\fastapi`
- `docker-compose up -d or docker-compose up`

Destroy a cluster


- `docker-compose stop`



<h1>Usage</h1>
From root dir navigate to dir <b>client</b>. Run <b>producer.py</b> to start producing the messages. When prompted enter number of dummy messages that you want to be generated.

From root dir navigate to <b>fastapi/data</b> dir, where you can find the <b>data.json</b> file. FastAPI is collecting messages from producer and writing it to the <b>data.json</b> file on minute level.

Happy developing :)

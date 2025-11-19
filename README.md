[![Open in Codespaces](https://classroom.github.com/assets/launch-codespace-2972f46106e565e64193e422d61a12cf1da4916b45550586e14ef0a7c637dd04.svg)](https://classroom.github.com/open-in-codespaces?assignment_repo_id=21736415)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?hide_repo_select=true&ref=main&repo=1097878214&machine=standardLinuxXL)

 ### TP image

> [!IMPORTANT]
> Do not build the image if working with Github Codespaces. Just pull it from docker hub:   
> `docker pull jaeo/ggmd:tp-stream`

```sh
# Apple Silicon version
docker build --build-arg ARCH=arm64 -t jaeo/ggmd:tp-stream .

# Codespaces version
docker build -t jaeo/ggmd:tp-stream .
```


### Option A: Run Topology in Local Cluster

#### Terminal 1: Stream Producer

Start a container:

```sh
docker compose run --rm --name client -it client /bin/bash
```

Compile and start the stream producer:

```sh
cd /ggmd-storm-stream
mvn package

./startStream.sh tortoise 10 150 9001
```


#### Terminal 2: Storm Topology

Connect to container: 

```sh
docker compose exec -it client /bin/bash
```

Compile the storm topology:

```sh
cd /storm/examples/ggmd-storm-topology/
mvn package
```

Run topology in local cluster mode:

```sh
# run cluster for 120 secs
storm local --local-ttl 120 target/stormTP-0.1.jar \
      stormTP.topology.TopologyT1 9001 9005
```

#### Terminal 3: Topology consumer (listener)

Connect to container: 

```sh
docker compose exec -it client /bin/bash
```

Compile and run the listener:

```sh
cd /ggmd-storm-listner/
mvn package

./startListner.sh 9005
```

Close all terminals

--------------

### Option B: Run Topology in a Docker-based Storm Cluster

#### Terminal 1: Storm Cluster

* Start cluster: 

```sh
docker compose up
```

#### Terminal 2: Stream Producer

Connect to the client container:

```sh
docker compose exec client /bin/bash
```

Compile and start the stream producer:

```sh
cd /ggmd-storm-stream
mvn package
./startStream.sh tortoise 10 150 9001
```

#### Terminal 3: Storm Topology

Connect to the client container: 

```sh
docker compose exec -it client /bin/bash
```

> [!IMPORTANT]  
> Modify `src/main/java/stormTP/topology/TopologyT1.java`   
> Replace `new InputStreamSpout("127.0.0.1", portINPUT);` code with   
> `new InputStreamSpout("storm-client", portINPUT);`

Compile the storm topology:

```sh
cd /storm/examples/ggmd-storm-topology/
mvn package
```

Submit topology to cluster:

```sh
# run cluster for 120 secs
storm jar target/stormTP-0.1.jar \
      stormTP.topology.TopologyT1 9001 9005
```
Check the Storm UI at http://localhost:8081. 

> Are you using Github Codespaces? Look in the PORTS section to discover the hostname you have to use to connect to the Storm UI.

#### Terminal 4: Topology consumer (listener)

Connect to the client container: 

```sh
docker compose exec -it client /bin/bash
```

Compile and run the listener:

> [!IMPORTANT]   
> Modify file `ggmd-storm-listner/src/main/java/StreamListner.java`.   
> Replace `"127.0.0.1"` with `storm-client`

```sh
cd /ggmd-storm-listner/
mvn package

./startListner.sh 9005
```

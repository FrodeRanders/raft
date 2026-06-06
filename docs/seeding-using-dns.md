# Seeding a cluster using DNS

## Context
Development and tests of this software library has primarily been run on a single host with
individual Raft nodes communicating through different ports on the same host. This is not how
I expect Raft to be run in domain specific test or production environments. 

For development ergonomic reasons, running all nodes on the same host is perfect, but steps need
to be taken to accomodate a realistic setup. Therefore, this information relates to how a Raft 
cluster would operate in a production (or test) runtime environment. 

The production runtime environment I have in mind is OpenShift/Kubernetes, but for development
I aim for Docker containers.

## Running a Raft cluster in Docker containers

I suggest making the Raft nodes resolve a cluster-local SRV name. That gives us a development
model close to OpenShift/Kubernetes.

The shape I aim for is:

```
raft node container ─┐
raft node container ─┼── Docker user-defined network
raft node container ─┘
      │
      └── DNS name: _raft._tcp.raft-cluster.local
```

Each node starts with only:

```
clusterName = raft-cluster.local
serviceName = _raft._tcp
```

Each node resolves SRV records, gets a seed set, connects to one of them, and then lets Raft 
take it from there.

### Manually setting up a Docker user-defined network

This section documents how to run individual Raft nodes in a Docker network, but it 
does *not* solve how to effectively seed the Raft nodes. These steps allows individual 
Raft nodes to assume other Raft nodes use the same port, but you still have to provide
the cluster with an initial seed list.

Docker containers on a custom network use Docker’s embedded DNS server at 127.0.0.11.
Docker documents that containers on custom networks use this embedded DNS server and 
that unresolved external names are forwarded to the host-configured DNS servers.  ￼

For a simple local test network:

```bash
docker network create raft-net
```

Run nodes on the same internal port:

```bash
docker run -d \
  --name raft-1 \
  --network raft-net \
  prebuilt-image \
  --node-id raft-1 \
  --bind 0.0.0.0:7000 \
  --cluster-dns _raft._tcp.raft.local

docker run -d \
  --name raft-2 \
  --network raft-net \
  prebuilt-image \
  --node-id raft-2 \
  --bind 0.0.0.0:7000 \
  --cluster-dns _raft._tcp.raft.local

docker run -d \
  --name raft-3 \
  --network raft-net \
  prebuilt-image \
  --node-id raft-3 \
  --bind 0.0.0.0:7000 \
  --cluster-dns _raft._tcp.raft.local
```

Inside the Docker network, all containers can use the same port, for example 7000. You only 
need different host ports if you want to reach them individually from your host, for example 
by adding the option:

```bash
 -p 17001:7000
 -p 17002:7000
 -p 17003:7000
```

The Raft nodes will internally communicate using:

```bash
raft-1:7000
raft-2:7000
raft-3:7000
```

Docker’s embedded DNS is useful for container-name lookup and service discovery on user-defined 
networks, but it is not a general-purpose DNS zone server where you can define arbitrary SRV 
records. Docker’s built-in discovery mainly gives you names such as:

```
raft-1
raft-2
raft-3
```

resolving to container IPs.

We still need to provide a seed list like: ```raft-1:7000,raft-2:7000,raft-3:7000```

### Manually setting up Docker for using DNS SRV records (for seeding) 

The cleanest approach to serve DNS SRV records for seeding is to run CoreDNS 
as a container inside the same Docker network. 

Example Corefile:

```
raft.local:53 {
    file /etc/coredns/raft.local.db
    log
    errors
}

.:53 {
    forward . 1.1.1.1 8.8.8.8
    log
    errors
}
```

Example zone file raft.local.db:

```dns
$ORIGIN raft.local.
$TTL 5
@       IN SOA  ns.raft.local. hostmaster.raft.local. (
            2026060301 ; serial
            5          ; refresh
            5          ; retry
            60         ; expire
            5          ; minimum
        )
        
        IN NS   ns.raft.local.

ns      IN A    10.77.0.10

raft-1  IN A    10.77.0.11
raft-2  IN A    10.77.0.12
raft-3  IN A    10.77.0.13

_raft._tcp IN SRV 10 10 7000 raft-1.raft.local.
_raft._tcp IN SRV 10 10 7000 raft-2.raft.local.
_raft._tcp IN SRV 10 10 7000 raft-3.raft.local.
```

We can create a Docker network with predictable IPs:

```bash
docker network create \
  --subnet 10.77.0.0/24 \
  raft-net
```

Run CoreDNS:

```bash
docker run -d \
  --name raft-dns \
  --network raft-net \
  --ip 10.77.0.10 \
  -v "$PWD/Corefile:/etc/coredns/Corefile:ro" \
  -v "$PWD/raft.local.db:/etc/coredns/raft.local.db:ro" \
  coredns/coredns:latest \
  -conf /etc/coredns/Corefile
```

Run the Raft nodes with CoreDNS as their resolver:

```bash
docker run -d \
  --name raft-1 \
  --network raft-net \
  --ip 10.77.0.11 \
  --dns 10.77.0.10 \
  prebuilt-image \
  --node-id raft-1 \
  --bind 0.0.0.0:7000 \
  --cluster-srv _raft._tcp.raft.local

docker run -d \
  --name raft-2 \
  --network raft-net \
  --ip 10.77.0.12 \
  --dns 10.77.0.10 \
  prebuilt-image \
  --node-id raft-2 \
  --bind 0.0.0.0:7000 \
  --cluster-srv _raft._tcp.raft.local

docker run -d \
  --name raft-3 \
  --network raft-net \
  --ip 10.77.0.13 \
  --dns 10.77.0.10 \
  prebuilt-image \
  --node-id raft-3 \
  --bind 0.0.0.0:7000 \
  --cluster-srv _raft._tcp.raft.local
```

From inside a container:

```bash
dig SRV _raft._tcp.raft.local
```

should return:

```
10 10 7000 raft-1.raft.local.
10 10 7000 raft-2.raft.local.
10 10 7000 raft-3.raft.local.
```

This is the closest local analogue to what Kubernetes/OpenShift gives you with headless 
services.

### Using Docker Compose to setup a DNS SRV record serving environment

For development, I guess using Docker Compose makes sense. Something like:

```yaml
services:
  dns:
    image: coredns/coredns:latest
    container_name: raft-dns
    command: ["-conf", "/etc/coredns/Corefile"]
    volumes:
      - ./Corefile:/etc/coredns/Corefile:ro
      - ./raft.local.db:/etc/coredns/raft.local.db:ro
    networks:
      raft-net:
        ipv4_address: 10.77.0.10
        
  raft-1:
    image: my-raft-image
    container_name: raft-1
    command:
      - --node-id=raft-1
      - --bind=0.0.0.0:7000
      - --cluster-srv=_raft._tcp.raft.local
    dns:
    - 10.77.0.10
    networks:
      raft-net:
        ipv4_address: 10.77.0.11
    ports:
      - "17001:7000"

  raft-2:
    image: my-raft-image
    container_name: raft-2
    command:
      - --node-id=raft-2
      - --bind=0.0.0.0:7000
      - --cluster-srv=_raft._tcp.raft.local
    dns:
      - 10.77.0.10
    networks:
      raft-net:
        ipv4_address: 10.77.0.12
    ports:
      - "17002:7000"
    
  raft-3:
    image: my-raft-image
    container_name: raft-3
    command:
      - --node-id=raft-3
      - --bind=0.0.0.0:7000
      - --cluster-srv=_raft._tcp.raft.local
    dns:
      - 10.77.0.10
    networks:
      raft-net:
        ipv4_address: 10.77.0.13
    ports:
      - "17003:7000"
    
  networks:
    raft-net:
      ipam:
        config:
          - subnet: 10.77.0.0/24
```

### On using static IPs versus Docker names

You do not strictly need static container IPs. You could let Docker assign addresses and have 
CoreDNS point to Docker names only if CoreDNS can resolve them through Docker’s DNS, but that 
becomes awkward because CoreDNS itself then needs to forward or resolve those names.

For a small local Raft cluster, static IPs are simpler:

```
raft-1.raft.test -> 10.77.0.11
raft-2.raft.test -> 10.77.0.12
raft-3.raft.test -> 10.77.0.13
```

However, the Raft node identity should not be the IP address. We want stable logical IDs, like:

```
raft-1
raft-2
raft-3
```

and treat addresses as current contact endpoints, which lets us move to OpenShift later, 
where the stable identity may become:
```
raft-0.raft-headless.my-namespace.svc.cluster.local
raft-1.raft-headless.my-namespace.svc.cluster.local
raft-2.raft-headless.my-namespace.svc.cluster.local
```

## OpenShift/Kubernetes equivalent

In OpenShift/Kubernetes, the natural construct is a StatefulSet plus a headless Service. 
Kubernetes creates SRV records for named ports in Services; for a headless Service, 
SRV records resolve to multiple answers, one per backing Pod, with the port and Pod DNS name.  ￼ OpenShift documentation describes the same pattern for discovering running pods via DNS SRV records using a headless service.  ￼

A Kubernetes/OpenShift Service might look like:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: raft
spec:
  clusterIP: None
  selector:
    app: raft
  ports:
    - name: raft
      protocol: TCP
      port: 7000
      targetPort: 7000
```

Then the SRV query is: ```_raft._tcp.raft.<namespace>.svc.cluster.local```

For a headless service, that should resolve to pod-specific targets, conceptually like:
```
10 33 7000 raft-0.raft.<namespace>.svc.cluster.local.
10 33 7000 raft-1.raft.<namespace>.svc.cluster.local.
10 33 7000 raft-2.raft.<namespace>.svc.cluster.local.
```

This maps well to the intended abstraction:

```
cluster name -> SRV seed set -> connect to any node -> Raft discovers rest
```

## Raft node startup logic

Raft node startup is approximately implemented like this:
```
Configuration:
    nodeId
    bindHost
    bindPort
    clusterSrvName
    optionalAdvertisedHost
    optionalAdvertisedPort

Startup:
    bind local Raft port
    resolve clusterSrvName
    shuffle returned SRV targets
    for each target:
        connect
        ask for cluster metadata
        verify clusterId
        learn current leader
        learn committed membership
        stop treating DNS as authoritative

If no seed answers:
    either start as bootstrap node, if explicitly allowed
    or fail fast

```

Important: do not silently create a new cluster just because DNS resolution fails. 
Make bootstrap explicit, for example:

```
--bootstrap-new-cluster=true
```

or:

```
--initial-cluster-size=3
```

Otherwise, a DNS problem can accidentally become a cluster-formation problem.

# Suggested implementation progression

Three increments.

* First, move from loopback/different ports to Docker network/same port:

```
raft-1:7000
raft-2:7000
raft-3:7000
```

* Second, add SRV lookup support in Raft code, but test it against CoreDNS:

```
_raft._tcp.raft.test
```


* Third, mirror the same name shape in OpenShift:

```
_raft._tcp.raft.<namespace>.svc.cluster.local
```

That gives a nice portability boundary. 
Locally you as a developer own the zone file; in OpenShift, the platform owns the DNS records.

## Local development mode

For a local development setup, either start on localhost with a series of ports (the initial 
approach during development), or use Docker containers with:
- Docker Compose
- custom Docker bridge network
- CoreDNS container
- one SRV record set for the Raft seed set
- one container per Raft node
- same internal Raft port on every node
- optional host port mappings only for debugging


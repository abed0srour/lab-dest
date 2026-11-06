# Chapter 10 – Phase 7 (Bonus): Load Balancing and Fault Tolerance

In Word, Phase 7 is **Chapter 10**, so its figures are **Figure 10.x**. Rewrite the paragraphs in your own words.

---

## 10.1 Objective

In this phase I removed the remaining single points of failure and made the system observable. That meant two gateways behind a load balancer, a message queue that shares background jobs between several workers, a replicated metadata database with automatic failover, Kubernetes for the storage nodes, and monitoring with Prometheus and Grafana.

## 10.2 What I Did

**Load balancer.** I now run two gateways and put nginx in front of them on port 8080. nginx sends requests to the two gateways in turn. If one stops answering, nginx skips it and sends the request to the other, so a crashed gateway causes no failed requests.

**RabbitMQ for background jobs.** Repairs, rebalancing, garbage collection and scrubbing are now distributed through RabbitMQ. Each job is still recorded in PostgreSQL, so its state, progress and traffic are kept, but a dispatcher hands waiting jobs to four RabbitMQ queues. Two worker replicas take jobs from these queues. A job is acknowledged only once it's finished: if a worker dies in the middle of a job, RabbitMQ gives the job to the other worker.

**Rebalancing and garbage collection.** When a new storage node joins, or a failed one comes back, a worker compares every chunk with the hash ring. It then moves the copies that now belong on another node. Copies that are no longer needed (moved away, or from deleted files) are removed by garbage-collection jobs. A blob that was written only seconds ago is never deleted, so an upload or repair still in progress can't lose its data.

**Replicated PostgreSQL.** The metadata database now runs on two nodes: a primary and a standby that receives every change by streaming replication. If the primary stops answering for 10 seconds, the standby promotes itself to primary. When the old primary comes back, it sees the other node is already primary and rejoins as its standby, so there are never two primaries. The services list both database hosts and always connect to the one that is currently primary.

**Monitoring.** Every service publishes metrics, including request counts and latency, bytes uploaded and downloaded, disk usage per node, degraded objects, and finished jobs and repair traffic per worker. Prometheus collects them every 5 seconds, and a Grafana dashboard with 21 panels shows them live.

**Kubernetes.** I wrote Kubernetes manifests and deployed the system on the Kubernetes cluster of Docker Desktop. The storage nodes are a StatefulSet: each of the 7 pods keeps a fixed name and its own persistent volume. Each pod has two probes:
- a **liveness** probe: if the node stops answering, Kubernetes restarts it;
- a **readiness** probe: a node whose disk isn't writable receives no traffic.

---

## 10.3 Screenshots

Start fresh: `docker compose down -v`, then `docker compose up -d --build` (wait about a minute).
Then run the first demo; it stops after each step:
`powershell -ExecutionPolicy Bypass -File scripts\demo_phase7.ps1`

**10.3.1 All the Services**
*Where:* demo, step 1.
*Before:* "The system now runs 20 containers, including two gateways, a load balancer, two workers, RabbitMQ, two database nodes, Prometheus and Grafana."
📷 **Figure 10.1: All the services of the system running**
*After:* "Every service is up, and the ones with a health check report healthy."

**10.3.2 Load Balancing**
*Where:* demo, step 2.
*Before:* "I sent requests through the load balancer, then killed one gateway in the middle of 40 requests."
📷 **Figure 10.2: Requests alternate between the gateways, and none fails when one is killed**
*After:* "The responses alternate between gateway-1 and gateway-2. After gateway-1 died, all the remaining requests went to gateway-2, and not a single one failed."

**10.3.3 The RabbitMQ Queues**
*Where:* browser → `http://127.0.0.1:15672` (user `dos`, password `dos`) → **Queues and Streams**, while step 3 of the demo is repairing.
*Before:* "RabbitMQ holds one queue per kind of background job."
📷 **Figure 10.3: The job queues in RabbitMQ**
*After:* "During the repair, messages flow through the jobs.repair queue and the two workers consume them."

**10.3.4 Repair Shared by Two Workers**
*Where:* demo, step 3.
*Before:* "I killed a storage node, which created hundreds of repair jobs."
📷 **Figure 10.4: A failed node repaired by two workers through RabbitMQ**
*After:* "The node was detected as failed, the repair finished in a few seconds, and the two workers did almost exactly half of the repairs each."

**10.3.5 The Node Comes Back**
*Where:* demo, step 4.
*Before:* "When the node came back, the system started three kinds of jobs by itself."
📷 **Figure 10.5: Scrub, garbage collection and rebalancing after the node rejoins**
*After:* "The node was scrubbed, its orphan copies collected, and its share of the data moved back onto it."

**10.3.6 Database Failover**
*Where:* demo, step 5.
*Before:* "The standby database receives every change from the primary. I killed the primary and uploaded files at the same time."
📷 **Figure 10.6: The primary database fails and the standby takes over**
*After:* "After 10 seconds the standby promoted itself. The upload that was in progress waited about 15 seconds and then succeeded instead of failing."

**10.3.7 The Old Primary Returns**
*Where:* demo, step 6.
*Before:* "Then I started the old primary again."
📷 **Figure 10.7: The old primary rejoins as a standby**
*After:* "It saw that the other node was already the primary, so it copied it and became its standby. The system never had two primaries."

**10.3.8 Adding a Storage Node**
*Where:* demo, step 7.
*Before:* "Finally, I added an eighth storage node to the running cluster."
📷 **Figure 10.8: Rebalancing moves part of the data onto the new node**
*After:* "About 800 chunks had one copy moved to the new node in about 9 seconds, so it now holds its fair share, close to one eighth."

**10.3.9 Prometheus Targets**
*Where:* browser → `http://127.0.0.1:9090/targets`.
*Before:* "Prometheus collects the metrics of every service."
📷 **Figure 10.9: Every service monitored by Prometheus**
*After:* "The gateways, storage nodes, metadata service, workers and RabbitMQ are all UP."

**10.3.10 The Grafana Dashboard**
*Where:* browser → `http://127.0.0.1:3000`, ideally during step 3 or step 7 of the demo, while something is happening.
*Before:* "Grafana shows the metrics as a live dashboard."
📷 **Figure 10.10: The Grafana dashboard: throughput, latency, disk usage per node, degraded objects and repair rate**
*After:* "During the failure, the degraded objects rise and fall back to zero while the repair rate peaks."

**Then run the Kubernetes demo:**
`powershell -ExecutionPolicy Bypass -File scripts\demo_phase7_k8s.ps1`

**10.3.11 Deployment on Kubernetes**
*Where:* Kubernetes demo, step 1.
*Before:* "I deployed the system on Kubernetes with the manifests in deploy/k8s."
📷 **Figure 10.11: The StatefulSet, Deployments and Services in Kubernetes**
*After:* "The storage nodes run as a StatefulSet, and the gateway is reached through a Service."

**10.3.12 Storage Pods and Volumes**
*Where:* Kubernetes demo, step 2.
📷 **Figure 10.12: Seven storage pods, each with its own persistent volume**
*After:* "Each pod has a fixed name and a volume bound only to it."

**10.3.13 Using the System on Kubernetes**
*Where:* Kubernetes demo, step 3.
📷 **Figure 10.13: An upload through the gateway Service, spread over two gateway pods**
*After:* "The upload succeeded, and the Service shared 20 requests between the two gateway pods."

**10.3.14 Liveness Probe**
*Where:* Kubernetes demo, step 4.
*Before:* "I froze one storage node so it stopped answering."
📷 **Figure 10.14: Kubernetes restarts a frozen node after its liveness probe fails**
*After:* "The readiness probe took the node out of service, then the liveness probe failed three times and Kubernetes restarted the container. Its data was still there."

**10.3.15 Recreating a Pod**
*Where:* Kubernetes demo, step 5.
📷 **Figure 10.15: A deleted pod comes back with the same name and the same volume**
*After:* "The StatefulSet recreated storage-node-3 in a few seconds, bound to the same volume and with the same blobs."

---

## 10.4 Results and Observations

| Test | Result |
|---|---|
| Gateway killed during 40 requests | 0 failed requests |
| Node failure, repair through RabbitMQ | Repairs split 366 / 360 between two workers |
| Database primary killed | Standby promoted after 10 s; the write in progress waited 15 s and succeeded |
| New storage node added | About 800 chunks moved to it in 9 s |
| Kubernetes node frozen | Restarted by the liveness probe after about 50 s, data kept |
| Kubernetes pod deleted | Recreated with the same volume and data |

**Observations.**
- **Database failover takes about 10 seconds.** That's a choice. Waiting longer avoids promoting the standby during a short network hiccup, but it means writes pause for longer.
- **Replication is asynchronous,** so a change made in the last instant before a crash could be lost.
- **No automatic arbiter:** with only two database nodes, a real production system would add one (for example etcd with Patroni) to decide safely which node is primary.
- **Rate limiting is per gateway.** With two gateways, each key effectively gets twice its limit. A shared counter, for example in Redis, would make it global.

| Task 7 requirement | Evidence |
|---|---|
| Nginx in front of at least 2 gateways | Figures 10.1, 10.2 |
| RabbitMQ queues repair, rebalancing and garbage-collection jobs across workers | Figures 10.3, 10.4, 10.5, 10.8 |
| Storage nodes as a Kubernetes StatefulSet with persistent volumes, liveness and readiness probes | Figures 10.11–10.15 |
| Replicated metadata store, not a single point of failure | Figures 10.6, 10.7 |
| Prometheus and Grafana: throughput, latency, disk per node, degraded objects, repair rate | Figures 10.9, 10.10 |

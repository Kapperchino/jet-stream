# Jet stream
A distributed streaming system made in golang. Pretty much trying to be like kafka but faster and easier to manage
Currently still very early in so don't expect anything to work.
## Abstract
So this is a Kafka copy but designed a bit differently. Partitions will only be able to exist in shard groups, which 
means the replication count is based on how many replicas a shard group has. I also use memberlist for shard group to shard group
communication, mainly has meta-data. Design doc coming soon. So plan is to write to the leader node and read from the follower nodes,
so publish will go to the leader node, and consume will consume from the follower nodes.
## What works
### Single node
On a single node, producing and consuming works fine. Consuming might still need a bit of work.
### One raft group
A raft group is a group of nodes that has one leader and multiple followers.
Producing works on this one but for consuming, more work will need to be put in as the raft groups doesn't support read operations,
so a consume call will be considered a write.
### More than one raft group
So this is getting into sharding, and currently it's not implemented as it needs a client. Cluster membership on the 
other hand works fine but needs more testing. I used memberlist for cluster membership across raft groups, and each node
saves cluster meta-data and updates it accordingly.
## Design
TODO
## Contribution
Plz help + TODO instructions
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
On a single node, producing and consuming are both solid
### One raft group
A raft group is a group of nodes that has one leader and multiple followers.
One raft group is quite solid, still need resiliency improvements though
### More than one raft group
So this currently works well locally, but like the above lacks resiliency, for example currently there's only ack for consuming and
not publishing,which means if a publishing fails but other succeeds, the publishing will go through on the nodes that did succeed.
## Design
TODO
## Contribution
Plz help + TODO instructions
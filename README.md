# Introduction

**IMPORTANT**
 This version of NkDIST is a complete rewrite, with a fully different API.
 
NkDIST is an library to register and manage Erlang processes in a [_riak_core_](https://github.com/basho/riak_core) cluster. Any Erlang process can register itself with NkDIST under a specifc _Class_ and _ObjId_, calling [`nkdist:register/3,4`](src/nkdist.erl) and selecting one of the following patterns:

Pattern|Desc
---|---
`reg`|The process is registered at a specific vnode living at a specific node in the cluster, selected using consistent hashing over the _Class_ and _ObjId_. The registration fails if any other process has already registered the same _Class_ and _ObjId_. Any process in any node of the cluster can now find the process' pid() and metadata calling `nkdist:get/2,3`. When the process stops, or calls `nkdist:unregister/2,3`, it is unregistered at the vnode.
`mreg`|Works in a very similar way than `reg`, but multiple registrations for the same _Class_ and _ObjId_ are allowed.
`proc`|Works in a similar way to `reg`, but it is intended for processes that should be evenly distributed in the cluster. If the process is registered at a different node than where its vnode is currently living, `must_move` message is sent to the process, and it must _move_ to the selected node (meaning starting a new proceess at the indicated node, registering with the same name using `replace_pid` options and stopping here). You can call `nkdist:get_vnode/2,3` to know the final node in advance. When new nodes are added or removed to the cluster, vnodes can move around, and new messages `must_move` are sent to this kind of processes for them to move along the vnodes.
`master`|It is intended for processes that run in every node of the cluster, so multiple registrations are allowed. It is very similar to `mreg`, but any time a process is added or removed a _master_ is elected, and every registered process is informed. This is an _eventualy consistent_ master election, so, under network partitions, several processes can be elected as masters.
`leader`|It is very similar to `master`, but NkDIST uses a strong consistent leader election (based on [riak_ensemble](https://github.com/basho/riak_ensemble)). Only the instance of process living in the same node as the riak_ensemble's root leader will receive the `leader` message (if it exists at all). The leader, if elected, is always unique, under any circunstances.

Once registered, the process can receive the following messages from NkDIST, depending on its type:

Message|Patterns|Dest
---|---|---
`{nkdist, {vnode_pid, pid()}}`|all|The vnode that has registered this process sends its pid(). You can monitor it, and register again in case it fails. If the vnode is moved, a new message will be sent, and you should unregister the old one and monitor the new one.
`{nkdist, {master, pid()}}`|`master`|A new master (eventually consistent) has been elected.
 `{leader, pid()|none}`|`leader`|A new leader has been elected, or no leader is currently available. In the later case, you can register again to force a new election.
 `{must_move, node()}`|`proc`|A registered `proc` process must move itself to the indicated node. It must start a new process ant the new node, registering again (use the `replace_pid` option to avoid a conflict with its old version) and stop here. The handoff process will not stop until all registered processes of this type have stopped.
  `{pid_conflict, pid()}`|`reg`, `proc`|During a handoff (vnode translation), a registration failed because there were another process already registered. You must send any update to the indicated process, and stop.
 `{type_conflict, reg_type()}`|all|During a handoff, a registration failed because there was another process already registered under a different pattern. You must stop
 
## Handoffs
 
 When new nodes are added or removed from the _riak core_ cluster, _vnodes_ can move from one node to another. Registrations are always available using `nkdist:get/2,3`, and the system is also capable of receiving new registrations at any moment. You can use the `{vnode_pid, pid()}` messages to notice vnode changes. Registrations with pattern `proc` should move along the vnode as soon as possible (after receiving the `{must_move, node()}` message), as the handoff process will not stop until all processes have moved.
 
## Network splits
 
 If the cluster is splitted into two or more sections, all vnodes will be started at each section, and several registrations for the same _Class_ and _ObjId_ can be received at each side. When the cluster is joined again, registrations belonging to the same vnodes are joined. In case of `mreg`, `master` and `leader` patterns, they are simply added. For `reg` and `proc`, since multiple registrations are not allowed, a _winning_ one will be selected, and the message `{pid_conflict, pid()}` will be sent to the other, for it to stop (possibly after reconciling its state with the winning). 
 
 For all patterns, if another registration for another pattern exists, the message `{type_conflict, reg_type()}` will be sent and the process should stop.
 
 
## Node faillures
 
 If a node suddenly gets disconnected, all registrations in all vnodes in it will not be available any longer, until it comes back (or for ever, if it crashed). If this is not acceptable, processes must use the `{vnode_pid, pid()}` message to start monitoring its vnode, and in case it fails, register again. The registration will be accepted at a _secondary_ vnode. If the crashed node comes back, the _primary_ and _secondary_ vnodes will be joined. If it is not going to come back, you must remove it from the cluster.
 
## Masters and leaders
 
 NkDIST supports two patterns where you are supposed to start a registered process with the same _Class_ and _ObjId_ at every node in the cluster. In the case of `master` pattern, there will be always a master, who will receive the `{master, pid()}` message. If it fails or stops, another one will be selected inmediately. In the case of a network split, one _master_ will be elected at every side.
 
 You can also use the `leader` pattern, where a maximum of one process will be elected as master, even in case of network split. Only the process living in the same node of the _riak_ensemble's root ensemble leader_, if it exists, will be selected as master. If this process or this node is not running, a `none` leader is sent. Processes must periodically try to register theirselves again, so that a new check will be performed to see if `riak_ensemble` has selected a new leader for the _root_ ensemble, and a registered process exists at that node.
 
 
## Management
 
 You can use standard _riak_core_ admin utilities to manange NkDIST cluster. A full set of utilities, including adding and removing of nodes is included in [nkdist_admin.erl](src/nkdist_admin.erl). You need rebar3 to build NkDIST.
 
 
## Testing and Samples
 
 You can use `make dev1` to `make dev5` to start a set of nodes to test. After starting each node, make it join the cluster: `nkdist_admin:quick_join("dev1@127.0.0.1")`. There are some tests and a full example, useful for all patterns, in [nkdist_test.erl](src/nkdist_test.erl).
 
 
 ### Thanks
 Thanks to [Basho](http://basho.com), for the excellent [Riak Core](https://github.com/basho/riak_core), and [Heinz N. Gies](https://github.com/Licenser) ([Project FIFO](https://project-fifo.net)) for the port to it to R18 and rebar3 [riak_core_ng](https://github.com/project-fifo/riak_core).
 
 
 
 
 
 
 
 
 
 


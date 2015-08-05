# Introduction

NkDIST is an library to manage Erlang processes evenly distributed in a [_riak_core_](https://github.com/basho/riak_core) cluster. When you add or remove nodes from the cluster, NkDIST-based processes automatically _move_ to another node to rebalance the cluster.

Before starting processes, you must supply one or several callback modules, using the [nkdist_proc](src/nkdist_proc.erl) behaviour. You must implement the following callbacks:

Name|Desc
---|---
start/2|Called when the start of a new process has been requested for this node. You must start a new Erlang process and return its `pid()`.
start_and_join/2|Called when an existing process must be moved from an old node to a new node. You receive the `pid()` of the old (still running) process and must start a new process and recover its state from the old one.
join/2|Called when an existing process must be moved from an old node to a new node, but the process already exists in the new node. You must _join_ them.

This would be a very simple example of a callback module. See the included [nkdist_proc_sample.erl](src/nkdist_proc_sample.erl) for a more complete version:

```erlang
-module(sample).
-behaviour(gen_server).
-behaviour(nkdist_proc).

-export([start/2, start_and_join/2, join/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,   
         handle_cast/2, handle_info/2]).


%% nkdist_proc behaviour

start(ProcId, Args) ->
    gen_server:start_link(?MODULE, {proc_id, ProcId, Args}, []).

start_and_join(ProcId, OldPid) ->
    gen_server:start_link(?MODULE, {join, ProcId, OldPid}, []).

join(Pid, OldPid) ->
    gen_server:call(Pid, {join, OldPid}).


%% gen_server behaviour

init({proc_id, _ProcId, Data}) ->
    {ok, Data};
init({join, _ProcId, OldPid}) ->
    case gen_server:call(OldPid, freeze) of
        {ok, Data} -> 
            {ok, Data}
        _ ->
            {stop, could_not_start}
    end.

handle_call(freeze, _From, Data) ->
    {stop, normal, {ok, Data}, State};

handle_call({join, OldPid}, _From, Data) ->
    case gen_server:call(OldPid, freeze) of
        {ok, OldData} ->
            {reply, ok, max(OldData, Data)};	
        _ ->
            {reply, {error, could_not_join}, State}
    end.

handle_cast(_Msg, Data) ->
    {noreply, Data}.
    
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
```

Now, you can go to any node of the _riak_core_ cluster and start an instance of our process:

```erlang
> nkdist:start(proc1, sample, 0)
{ok, <...>}
```

NkDIST will find the corresponding _vnode index_ for this process (using _consistent hashing_ over the `proc_id()`, _proc1_ in this example) and will send a request to that vnode to start the process. The function `sample:start/2` will be called, and the returned `pid()` is replied to the caller and stored in a map in the vnode. If the process dies, it is removed from the vnode store.

Later on, you can find the `pid()` for the process calling `nkdist:find(proc1)`. Once a `pid()` is returned, it is cached at each node in a local ETS table.

When you add or remove nodes to the _riak_core_ cluster, the _handoff process_ starts and some vnodes must move from the old node to a new node. Each moving vnode will look at its managed processes, and will call `sample:start_and_join/2` at the new node, with the `pid()` of the old process. The callback function must start a new process, get any state from the old process and stop it as soon as possible.

In some rare circumstances (like a network split) it can happen that a process with an specific `proc_id()` is started at two or more nodes at the same time. NkDIST is an _eventually consistent system_, so, once the network is reconnected, _handoffs_ will occur, and, if a node having a process with an specific `proc_id()` receives the _handoff_ from another with the same id, the callback `sample:join/2` would be called. The surviving process receiving the _join_ call must get any state from the old node and reconcile it with its own state, and stop the old node as soon as possible.


## Faillure of nodes

If a node fails, all processes started at that node will be lost. You must save any important state in an external system (like [NkBASE](https://github.com/Nekso/nkbase)).

Until the moment where the faillure is detected by the _Erlang distributed system_, requests for any vnode index assigned to that node will fail. Once it is detected, a _secondary vnode_ will be created at another node. New processes assigned to this vnode index will be started at the new, _temporary_ node. When the failed node comes back (or it is removed from the cluster), started processes will be moved or joined at the new, primary vnode.




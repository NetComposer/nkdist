%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Main functions
-module(nkdist).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([register/3, register/4, unregister/2, unregister/3]).
-export([get_node/2, get_node/3, find/2, find/3, search_class/1]).
-export([enable_leader/0, node_leader/0]).

-export([find_proc/2, find_proc_in_vnode/2, start_proc/3, get_procs/0, get_procs/1]).
-export([register/1, get_registered/1, get_masters/0, get_vnode/1, get_vnode/2]).
-export([get_info/0]).
-export([dump/0]).
-export_type([obj_class/0, obj_key/0, obj_meta/0, obj_idx/0]).
-export_type([proc_id/0, vnode_id/0, reg_type/0]).
-export_type([nkdist_info/0]).

-include("nkdist.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type obj_class() :: term().

-type obj_key() :: term().

-type obj_meta() :: term().

-type obj_idx() :: <<_:160>>.

-type vnode_id() :: chash:index_as_int().

-type proc_id() :: term().

-type reg_type() :: reg | mreg | proc | master | leader.

-type nkdist_info() :: {nkdist, nkdist_msg()}.

-type nkdist_msg() ::
    {vnode_pid, pid()}          |   % The vnode has registered the request.
                                    % You should monitor de pid, and register again
                                    % in case it fails. If the vnode is moved,
                                    % a new message will be sent, and you should
                                    % unregister the old one
    {master, pid()}             |   % A new master has been elected for a 'master' 
                                    % registration
    {leader, pid()|none}        |   % A new leader has been elected for a 'leader'
                                    % registration, or none is available. In this case
                                    % the process should re-register itself to forcer
                                    % a new election try.
    {must_move, node()}         |   % A registered 'proc' process must move itself
                                    % and start ad register at this node, because 
                                    % its vnode halready moved.
    {pid_conflict, pid()}       |   % During a handoff, a reg or proc registration
                                    % failed because there was another proces
                                    % already registered. Must send any update to 
                                    % the pid() and stop.
    {type_conflict, reg_type()}.    % During a handoff, a registration
                                    % failed because there was another process
                                    % already registered with another type.
                                    % Must stop






%% ===================================================================
%% Public
%% ===================================================================


%% @doc Tries to register a process globally
-spec register(reg_type(), obj_class(), obj_key()) ->
    ok | {error, term()}.

register(Type, Class, ObjId) ->
    register(Type, Class, ObjId, #{}).


%% @doc Tries to register a process globally
-spec register(reg_type(), obj_class(), obj_key(), 
               #{pid=>pid(), meta=>obj_meta(), obj_idx=>obj_idx()}) ->
    ok | {error, term()}.

register(Type, Class, ObjId, Opts) ->
    Pid = case Opts of
        #{pid:=UserPid} -> UserPid;
        _ -> self()
    end,
    case get_node(Class, ObjId, Opts) of
        {ok, Node, VNodeId} ->
            Meta = maps:get(meta, Opts, undefined),
            nkdist_vnode:register({Node, VNodeId}, Type, Class, ObjId, Meta, Pid);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Tries to register a process globally
-spec unregister(obj_class(), obj_key()) ->
    ok | {error, term()}.

unregister(Class, ObjId) ->
    unregister(Class, ObjId, #{}).


%% @doc Tries to unregister a process globally
-spec unregister(obj_class(), obj_key(), #{pid=>pid(), obj_idx=>obj_idx()}) ->
    ok | {error, term()}.

unregister(Class, ObjId, Opts) ->
    Pid = case Opts of
        #{pid:=UserPid} -> UserPid;
        _ -> self()
    end,
    case get_node(Class, ObjId, Opts) of
        {ok, Node, VNodeId} ->
            nkdist_vnode:unregister({Node, VNodeId}, Class, ObjId, Pid);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Finds a the registered metadata and pid().
-spec find(obj_class(), obj_key()) ->
    {ok, reg_type(), [{obj_meta(), pid()}]} |
    {error, term()}.

find(Class, ObjId) ->
    find(Class, ObjId, #{}).


%% @doc Finds a the registered metadata and pid().
-spec find(obj_class(), obj_key(), #{obj_idx=>obj_idx()}) ->
    {ok, reg_type(), [{obj_meta(), pid()}]} |
    {error, term()}.

find(Class, ObjId, Opts) ->
    case get_node(Class, ObjId, Opts) of
        {ok, Node, VNodeId} ->
            nkdist_vnode:find({Node, VNodeId}, Class, ObjId);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets all registered keys for a class
-spec search_class(obj_class()) ->
    [obj_key()].

search_class(Class) ->
    Fun = fun(Data, Acc) -> Data++Acc end,
    nkdist_coverage:launch({get_class, Class}, 1, 10000, Fun, []).


%% @doc Equivalent to get_vnode(Class, ObjId, #{})
-spec get_node(obj_class(), obj_key()) ->
    {ok, node(), vnode_id()} | {error, term()}.

get_node(Class, ObjId) ->
    get_node(Class, ObjId, #{}).


%% @doc Finds the assigne node and vnode for any object id
-spec get_node(obj_class(), obj_key(), #{obj_idx=>obj_idx()}) ->
    {ok, node(), vnode_id()} | {error, term()}.

get_node(Class, ObjId, Opts) ->
    DocIdx = case Opts of
        #{doc_idx:=UserIdx} -> 
            UserIdx;
        _ -> 
            chash:key_of({Class, ObjId})
    end,
    % We will get the associated IDX to this process, with a node that is
    % currently available. 
    % If it is a secondary vnode (the node with the primary has failed), 
    % a handoff process will move the process back to the primary
    case riak_core_apl:get_apl(DocIdx, 1, nkdist) of
        [{Idx, Node}] -> {ok, Idx, Node};
        [] -> {error, vnode_not_ready}
    end.



%% @doc Enables de consensus subsystem.
%% This should only be performed in a single node in the cluster.
%% The recommended way is letting enable the subsystem using the 
%% 'enable_consensus' riak_core parameter, and waiting to have three nodes 
%% in the cluster.
%%
%% After the subsystem is enabled, riak_core will take care of add every 
%% new cluster member to the ensemble cluster and as a member of the 'root'
%% ensemble, and also removing leaving nodes.
-spec enable_leader() ->
    ok | {error, term()}.

enable_leader() ->
    case riak_ensemble_manager:enabled() of
        true ->
            ok;
        false ->
            case riak_ensemble_manager:enable() of
                ok -> 
                    ok;
                error -> 
                    {error, could_not_enable}
            end
    end.


%% @doc Get current node leader
node_leader() ->
    case riak_ensemble_manager:get_leader(root) of
        undefined -> undefined;
        {_, Node} -> Node
    end.











%% @doc Finds a process pid
-spec find_proc(module(), proc_id()) ->
    {ok, pid()} | {error, not_found} | {error, term()}.

find_proc(CallBack, ProcId) ->
    case nklib_proc:values({nkdist, CallBack, ProcId}) of
        [{_, Pid}|_] ->
            {ok, Pid};
        [] ->
            find_proc_in_vnode(CallBack, ProcId)
    end.


%% @doc Finds a process pid directly in vnode
%% (without using the cache)
-spec find_proc_in_vnode(module(), proc_id()) ->
    {ok, pid()} | {error, not_found} | {error, term()}.

find_proc_in_vnode(CallBack, ProcId) ->
	case get_vnode(CallBack, ProcId) of
        {ok, VNodeId} ->
            case nkdist_vnode:find_proc(VNodeId, CallBack, ProcId) of
            	{ok, Pid} ->
            		nklib_proc:put({nkdist, CallBack, ProcId}, VNodeId, Pid),
            		{ok, Pid};
            	{error, Error} ->
            		{error, Error}
            end;
        error ->
            {error, no_vnode}
    end.


%% @doc Starts a new process 
-spec start_proc(module(), proc_id(), term()) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.

start_proc(CallBack, ProcId, Args) ->
    case get_vnode(CallBack, ProcId) of
        {ok, VNodeId} ->
            case nkdist_vnode:start_proc(VNodeId, CallBack, ProcId, Args) of
            	{ok, Pid} ->
            		nklib_proc:put({nkdist, CallBack, ProcId}, VNodeId, Pid),
            		{ok, Pid};
            	{error, {already_started, Pid}} ->
            		nklib_proc:put({nkdist, CallBack, ProcId}, VNodeId, Pid),
                    {error, {already_started, Pid}};
                {error, Error} ->
                    {error, Error}
            end;
        error ->
            {error, no_vnode}
    end.


%% @doc Gets all stared processes in the cluster
-spec get_procs() ->
    {ok, [{{module(), proc_id()}, pid()}]}.

get_procs() ->
    Fun = fun(Data, Acc) -> Data++Acc end,
    nkdist_coverage:launch(get_procs, 1, 10000, Fun, []).


%% @doc Gets all started processes in the cluster belonging to this callback
-spec get_procs(module()) ->
    {ok, [{proc_id(), pid()}]}.

get_procs(CallBack) ->
	Fun = fun(Data, Acc) -> Data++Acc end,
	nkdist_coverage:launch({get_procs, CallBack}, 1, 10000, Fun, []).


%% @doc Registers a master class
%% NkDIST will keep at a specific VNode the list of pids of all 
%% processes calling this function, and will send to all the message 
%% {nkdist_master, Class, pid()}, with the pid of the first
%% successfully registered process.
%% If this dies, the next one will be selected and sent to all.
%% See nkdist_gen_server
-spec register(atom()) ->
    {ok, VNode::pid()} | {error, term()}.

register(Class) ->
    case get_vnode(Class) of
        {ok, VNodeId} ->
            nkdist_vnode:register(VNodeId, Class, self());
        error ->
            {error, no_vnode}
    end.


%% @doc Registers a master class
%% NkDIST will keep at a specific VNode the list of pids of all 
%% processes calling this function, and will send to all the message 
%% {nkdist_master, Class, pid()}, with the pid of the first
%% successfully registered process.
%% If this dies, the next one will be selected and sent to all.
%% See nkdist_gen_server
-spec get_registered(atom()) ->
    {ok, [pid()]} | {error, term()}.

get_registered(Class) ->
    case get_vnode(Class) of
        {ok, VNodeId} ->
            nkdist_vnode:get_registered(VNodeId, Class);
        error ->
            {error, no_vnode}
    end.


%% @doc Gets all started masters in the cluster
-spec get_masters() ->
    {ok, #{atom() => [pid()]}}.

get_masters() ->
    Fun = fun(Map, Acc) -> maps:merge(Acc, Map) end,
    nkdist_coverage:launch(get_masters, 1, 10000, Fun, #{}).







%% @doc Gets all stared processes in the cluster
-spec dump() ->
    {ok, [{{module(), proc_id()}, pid()}]}.

dump() ->
    Fun = fun(Data, Acc) -> [Data|Acc] end,
    nkdist_coverage:launch(dump, 1, 10000, Fun, []).





%% ===================================================================
%% Private
%% ===================================================================


%% @private
-spec get_vnode(term()) ->
    {ok, vnode_id()} | error.

get_vnode(Term) ->
    get_vnode(undefined, Term).

%% @private
-spec get_vnode(module(), term()) ->
    {ok, vnode_id()} | error.

get_vnode(Module, Term) ->
    DocIdx = case 
        Module/=undefined andalso erlang:function_exported(Module, nkdist_hash, 1) 
    of
        true ->
            Module:nkdist_hash(Term);
        false ->
            chash:key_of(Term)
    end,
    % We will get the associated IDX to this process, with a node that is
    % currently available. 
    % If it is a secondary vnode (the node with the primary has failed), 
    % a handoff process will move the process back to the primary
    case riak_core_apl:get_apl(DocIdx, 1, nkdist) of
        [{Idx, Node}] -> {ok, {Idx, Node}};
        [] -> error
    end.










%% @private
get_info() ->
    Fun = fun(Map, Acc) -> [Map|Acc] end,
    case nkdist_coverage:launch(get_info, 1, 10000, Fun, []) of
        {ok, List} -> lists:sort(List);
        {error, Error} -> {error, Error}
    end.




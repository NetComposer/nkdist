%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Carlos Gonzalez Florido.  All Rights Reserved.
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

-export([register/3, register/4, unregister/2, unregister/3, unregister_all/2]).
-export([get_vnode/2, get_vnode/3, get/2, get/3, get_objs/1]).
-export([gen_server_start/6]).
-export([enable_leader/0, node_leader/0]).
-export_type([reg_type/0, obj_class/0, obj_key/0, obj_meta/0, obj_idx/0]).
-export_type([vnode_id/0]).
-export_type([nkdist_info/0]).

-include("nkdist.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type reg_type() :: reg | mreg | proc | master | leader.

-type obj_class() :: term().

-type obj_key() :: term().

-type obj_meta() :: term().

-type obj_idx() :: <<_:160>>.

-type get_opts() ::
    #{
        idx => obj_idx()
    }.

-type reg_opts() ::
    #{
        meta => term(),         % Associate this metadata to the registration
        pid => pid(),           % Registers this pid instead of self()
        replace_pid => pid(),   % Replaces this pid without generating conflict
        idx => obj_idx()    % Uses this idx to select the vnode instead of auto
    }.

-type unreg_opts() ::
    #{
        pid => pid(),           % Registers this pid instead of self()
        idx => obj_idx()    % Uses this idx to select the vnode
    }.

-type vnode_id() :: chash:index_as_int().

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
                                    % the process should re-register itself to force
                                    % a new election try.
    {must_move, node()}         |   % A registered 'proc' process must move itself
                                    % and start and register at this node, because
                                    % its vnode has been moved.
    {pid_conflict, pid()}       |   % During a handoff, a reg or proc registration
                                    % failed because there was another process
                                    % already registered. Must send any update to 
                                    % the pid() and stop.
    {type_conflict, reg_type()}.    % During a handoff, a registration
                                    % failed because there was another process
                                    % already registered with another type.
                                    % Must stop


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Tries to register a process
-spec register(reg_type(), obj_class(), obj_key()) ->
    ok | {error, term()}.

register(Type, Class, ObjId) ->
    register(Type, Class, ObjId, #{}).


%% @doc Tries to register a process
-spec register(reg_type(), obj_class(), obj_key(), reg_opts()) ->
    ok | {error, term()}.

register(Type, Class, ObjId, Opts) ->
    case get_vnode(Class, ObjId, Opts) of
        {ok, Node, VNodeId} ->
            nkdist_vnode:reg({VNodeId, Node}, Type, Class, ObjId, Opts);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Unregisters a process
-spec unregister(obj_class(), obj_key()) ->
    ok | {error, term()}.

unregister(Class, ObjId) ->
    unregister(Class, ObjId, #{}).


%% @doc Tries to unregister a process globally
-spec unregister(obj_class(), obj_key(), unreg_opts()) ->
    ok | {error, term()}.

unregister(Class, ObjId, Opts) ->
    case get_vnode(Class, ObjId, Opts) of
        {ok, Node, VNodeId} ->
            nkdist_vnode:unreg({VNodeId, Node}, Class, ObjId, Opts);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Unregisters all processes for this class and id
-spec unregister_all(obj_class(), obj_key()) ->
    ok | {error, term()}.

unregister_all(Class, ObjId) ->
    case get_vnode(Class, ObjId, #{}) of
        {ok, Node, VNodeId} ->
            nkdist_vnode:unreg_all({VNodeId, Node}, Class, ObjId);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets registration information
-spec get(obj_class(), obj_key()) ->
    {ok, reg_type(), [{obj_meta(), pid()}]} |
    {error, object_not_found|vnode_not_ready|term()}.

get(Class, ObjId) ->
    get(Class, ObjId, #{}).


%% @doc Gets registration information
-spec get(obj_class(), obj_key(), get_opts()) ->
    {ok, reg_type(), [{obj_meta(), pid()}]} |
    {error, term()}.

get(Class, ObjId, Opts) ->
    case get_vnode(Class, ObjId, Opts) of
        {ok, Node, VNodeId} ->
            nkdist_vnode:get({VNodeId, Node}, Class, ObjId);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets all registered keys for a class. Not safe for production.
-spec get_objs(obj_class()) ->
    {ok, [obj_key()]} | {error, term()}.

get_objs(Class) ->
    Fun = fun(Data, Acc) -> Data++Acc end,
    nkdist_coverage:launch({get_objs, Class}, 1, 10000, Fun, []).


%% @doc Equivalent to get_vnode(Class, ObjId, #{})
-spec get_vnode(obj_class(), obj_key()) ->
    {ok, node(), vnode_id()} | {error, term()}.

get_vnode(Class, ObjId) ->
    get_vnode(Class, ObjId, #{}).


%% @doc Finds the assigned node and vnode for any class and object id
-spec get_vnode(obj_class(), obj_key(), get_opts()) ->
    {ok, node(), vnode_id()} | {error, term()}.

get_vnode(Class, ObjId, Opts) ->
    DocIdx = case Opts of
        #{idx:=UserIdx} ->
            chash:key_of(UserIdx);
        _ -> 
            chash:key_of({Class, ObjId})
    end,
    % We will get the associated IDX to this process, with a node that is
    % currently available. 
    % If it is a secondary vnode (the node with the primary has failed), 
    % a handoff process will move the process back to the primary
    case riak_core_apl:get_apl(DocIdx, 1, nkdist) of
        [{Idx, Node}] -> 
            {ok, Node, Idx};
        [] -> 
            {error, vnode_not_ready}
    end.


%% @doc Starts a gen_server at the right node
-spec gen_server_start(obj_class(), obj_key(), get_opts(), module(), list(), list()) ->
    {ok, pid()} | {error, term()}.

gen_server_start(Class, ObjId, Opts, Module, Args, GenOpts) ->
    case nkdist:get_vnode(Class, ObjId, Opts) of
        {ok, Node, _Idx} ->
            rpc:call(Node, gen_server, start, [Module, Args, GenOpts]);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Enables de leader management subsystem (riak ensemble)
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


%% @doc Gets current node leader
-spec node_leader() ->
    node() | undefined.

node_leader() ->
    case riak_ensemble_manager:get_leader(root) of
        undefined -> undefined;
        {_, Node} -> Node
    end.




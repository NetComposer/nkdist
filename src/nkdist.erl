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

-export([find_proc/2, find_proc_in_vnode/2, start_proc/3, get_procs/0, get_procs/1]).
-export([register/1, get_registered/1, get_masters/0, get_vnode/1, get_vnode/2]).
-export([get_info/0]).

-export_type([proc_id/0, vnode_id/0]).
-include("nkdist.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type proc_id() :: term().
-type vnode_id() :: {chash:index_as_int(),  node()}.


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Finds a process pid
-spec find_proc(module(), proc_id()) ->
    {ok, pid()} | {error, not_found} | {error, term()}.

find_proc(CallBack, ProcId) ->
    case nklib_proc:values({?APP, CallBack, ProcId}) of
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
            		nklib_proc:put({?APP, CallBack, ProcId}, VNodeId, Pid),
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
            		nklib_proc:put({?APP, CallBack, ProcId}, VNodeId, Pid),
            		{ok, Pid};
            	{error, {already_started, Pid}} ->
            		nklib_proc:put({?APP, CallBack, ProcId}, VNodeId, Pid),
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
    case riak_core_apl:get_apl(DocIdx, 1, ?APP) of
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




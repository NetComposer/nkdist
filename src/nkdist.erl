%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Carlos Gonzalez Florido.  All Rights Reserved.
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

-export([find/1, find_in_vnode/1, start/3, get_all/0, get_all/1]).
-export([get_vnode/1]).

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
-spec find(proc_id()) ->
    {ok, pid()} | {error, not_found} | {error, term()}.

find(ProcId) ->
	case nklib_proc:values({?APP, ProcId}) of
		[{_, Pid}|_] ->
			{ok, Pid};
		[] ->
			find_in_vnode(ProcId)
	end.


%% @doc Finds a process pid directly in vnode
%% (without using the cache)
-spec find_in_vnode(proc_id()) ->
    {ok, pid()} | {error, not_found} | {error, term()}.

find_in_vnode(ProcId) ->
	VNodeId = get_vnode(ProcId),
    case nkdist_vnode:find_proc(VNodeId, ProcId) of
    	{ok, Pid} ->
    		nklib_proc:put({?APP, ProcId}, VNodeId, Pid),
    		{ok, Pid};
    	{error, Error} ->
    		{error, Error}
    end.


%% @doc Starts a new process 
-spec start(proc_id(), module(), term()) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.

start(ProcId, CallBack, Args) ->
	VNodeId = get_vnode(ProcId),
    case nkdist_vnode:start_proc(VNodeId, ProcId, CallBack, Args) of
    	{ok, Pid} ->
    		nklib_proc:put({?APP, ProcId}, VNodeId, Pid),
    		{ok, Pid};
    	{error, {already_started, Pid}} ->
    		nklib_proc:put({?APP, ProcId}, VNodeId, Pid),
            {error, {already_started, Pid}};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets all stared processes in the cluster
-spec get_all() ->
    {ok, [{proc_id(), module(), pid()}]}.


get_all() ->
	Fun = fun(Data, Acc) -> Data++Acc end,
	nkdist_coverage:launch(get_procs, 1, 10000, Fun, []).


%% @doc Gets all stared processes in the cluster belonging to this callback
-spec get_all(module()) ->
    {ok, [{proc_id(), pid()}]}.

get_all(CallBack) ->
	Fun = fun(Data, Acc) -> Data++Acc end,
	nkdist_coverage:launch({get_procs, CallBack}, 1, 10000, Fun, []).



%% ===================================================================
%% Private
%% ===================================================================


%% @private
-spec get_vnode(proc_id()) ->
    vnode_id().

get_vnode(ProcId) ->
	DocIdx = riak_core_util:chash_key({?APP, ProcId}),
	% We will get the associated IDX to this process, with a node that is
	% currently available. 
	% If it is a secondary vnode (the node with the primary has failed), 
	% a handoff process will move the process back to the primary
    [{Idx, Node}] = riak_core_apl:get_apl(DocIdx, 1, ?APP),
    {Idx, Node}.


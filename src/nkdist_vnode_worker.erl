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

%% @private Worker module for vnode
-module(nkdist_vnode_worker).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(riak_core_vnode_worker).

-export([init_worker/3, handle_work/3]).

-include("nkdist.hrl").

-record(state, {
	idx :: chash:index_as_int()
}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @private
init_worker(VNodeIndex, [], _Props) ->
    {ok, #state{idx=VNodeIndex}}.


%% @private
handle_work({handoff, Masters, Procs, Fun, Acc}, Sender, State) ->
	Acc1 = do_master_handoff(Masters, Fun, Acc),
	Acc2 = do_proc_handoff(Procs, Fun, Acc1),
	riak_core_vnode:reply(Sender, Acc2),
	{noreply, State}.



%%%===================================================================
%%% Internal
%%%===================================================================



%% @private
do_master_handoff([], _Fun, Acc) ->
	Acc;

do_master_handoff([{Name, Pids}|Rest], Fun, Acc) ->
	Acc1 = Fun({master, Name}, Pids, Acc),
	do_master_handoff(Rest, Fun, Acc1).


%% @private
do_proc_handoff([], _Fun, Acc) ->
	Acc;

do_proc_handoff([{{CallBack, ProcId}, Pid}|Rest], Fun, Acc) ->
	Acc1 = Fun({proc, CallBack, ProcId}, Pid, Acc),
	do_proc_handoff(Rest, Fun, Acc1).	
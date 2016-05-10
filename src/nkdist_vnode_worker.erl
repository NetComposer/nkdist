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

%% @private Worker module for vnode
-module(nkdist_vnode_worker).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(riak_core_vnode_worker).

-export([init_worker/3, handle_work/3]).

-include("nkdist.hrl").

-record(state, {
	idx :: chash:index_as_int()
}).


-define(PROC_RETRY, 5).
-define(PROC_MAX_WAIT, 4*60*60). 	% 4h


%% ===================================================================
%% Public API
%% ===================================================================

%% @private
init_worker(VNodeIndex, [], _Props) ->
    {ok, #state{idx=VNodeIndex}}.


%% @private
%% Dialyzer says 'Sender' is not a pid() like defined in riak_core_vnode_worker
%% (and it is right, the callback spec is wrong)
handle_work({handoff, Node, Ets, Fun, Acc}, Sender, State) ->
	Acc2 = handoff(ets:first(Ets), Node, Ets, Fun, Acc, []),
 	riak_core_vnode:reply(Sender, Acc2),
	{noreply, State}.



%%%===================================================================
%%% Internal
%%%===================================================================

handoff('$end_of_table', _Node, _Ets, _Fun, Acc, Wait) ->
	handoff_wait(Wait, nklib_util:timestamp() + ?PROC_MAX_WAIT),
	Acc;

handoff({mon, _}=Key, Node, Ets, Fun, Acc, Wait) ->
	handoff(ets:next(Ets, Key), Node, Ets, Fun, Acc, Wait);

handoff({obj, Class, ObjId}=Key, Node, Ets, Fun, Acc, Wait) ->
	Next = ets:next(Ets, Key),
	case ets:lookup(Ets, Key) of
		[{Key, proc, [{_Meta, Pid, _Mon}]}] ->
			Wait2 = case node(Pid) of
				Node -> 
					Wait;
				_ ->
					Pid ! {nkdist, {must_move, Node}},
					[Pid|Wait]
			end,
			handoff(Next, Node, Ets, Fun, Acc, Wait2);
		[{Key, Type, List}] ->
			List2 = [{Meta, Pid} || {Meta, Pid, _Mon} <- List],
			Acc2 = Fun({Class, ObjId}, {Type, List2}, Acc),
			handoff(Next, Node, Ets, Fun, Acc2, Wait);
		[] ->
			handoff(Next, Node, Ets, Fun, Acc, Wait)
	end.


%% @private
handoff_wait([], _Max) ->
	ok;

handoff_wait([Pid|Rest]=List, Max) ->
	case is_process_alive(Pid) of
		true ->
			case nklib_util:timestamp() > Max of
				true ->
					lager:warning("VNode exiting while waiting for 'proc' processes"),
					ok;
				false ->
					lager:info("VNode waiting for ~p 'proc' processes", 
							   [length(List)]),
					timer:sleep(1000*?PROC_RETRY),
					handoff_wait(List, Max)
			end;
		false ->
			handoff_wait(Rest, Max)
	end.




% %% @private
% do_master_handoff([], _Fun, Acc) ->
% 	Acc;

% do_master_handoff([{Name, Pids}|Rest], Fun, Acc) ->
% 	Acc1 = Fun({master, Name}, Pids, Acc),
% 	do_master_handoff(Rest, Fun, Acc1).


% %% @private
% do_proc_handoff([], _Fun, Acc) ->
% 	Acc;

% do_proc_handoff([{{CallBack, ProcId}, Pid}|Rest], Fun, Acc) ->
% 	Acc1 = Fun({proc, CallBack, ProcId}, Pid, Acc),
% 	do_proc_handoff(Rest, Fun, Acc1).	
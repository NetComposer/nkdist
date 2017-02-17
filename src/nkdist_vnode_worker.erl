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

%% @private Worker module for vnode
-module(nkdist_vnode_worker).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(riak_core_vnode_worker).

-export([init_worker/3, handle_work/3]).

-include("nkdist.hrl").


-define(PROC_RETRY, 5).
-define(PROC_MAX_WAIT, 4*60*60). 	% 4h

%% Dialyzer says 'Sender' is not a pid() like defined in riak_core_vnode_worker
%% (and it is right, the callback spec is wrong)
-dialyzer({nowarn_function, handle_work/3}).


-define(DEBUG(Txt, Args, State),
    case State#state.debug of
        true -> ?LLOG(debug, Txt, Args, State);
        _ -> ok
    end).


-define(LLOG(Type, Txt, Args, State),
    lager:Type(
            [
                {idx_pos, State#state.pos}
            ],
           "NkDIST vnode worker (~p): "++Txt, [State#state.pos | Args])).



-record(state, {
	idx :: chash:index_as_int(),
	pos :: integer(),
	ets :: ets:tid(),
	debug :: boolean(),
	node :: atom(),
	hfun,
	timeout
}).


%% ===================================================================
%% Public API
%% ===================================================================

%% @private
init_worker(VNodeIndex, [Ets, Pos, Debug], _Props) ->
	State = #state{idx=VNodeIndex, pos=Pos, ets=Ets, debug=Debug},
	?DEBUG("started (~p)", [self()], State),
    {ok, State}.


%% @private
handle_work({handoff, Node, Fun, Acc}, Sender, #state{ets=Ets}=State) ->
	?DEBUG("handoff starting to ~p", [Node], State),
	State2 = State#state{
		node = Node, 
		hfun = Fun,
		timeout = nklib_util:timestamp() + ?PROC_MAX_WAIT
	},
	Acc2 = handoff(ets:first(Ets), Acc, [], State2),
	?DEBUG("handoff finished", [], State2),
 	riak_core_vnode:reply(Sender, Acc2),
	{noreply, State2}.



%%%===================================================================
%%% Internal
%%%===================================================================

handoff('$end_of_table', Acc, Wait, State) ->
	wait_for_procs(Wait, State),
	Acc;

handoff({mon, _}=Key, Acc, Wait, #state{ets=Ets}=State) ->
	handoff(ets:next(Ets, Key), Acc, Wait, State);

handoff({obj, Class, ObjId}=Key, Acc, Wait, State) ->
	#state{ets=Ets, node=Node, hfun=Fun} = State,
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
			handoff(Next, Acc, Wait2, State);
		[{Key, Type, List}] ->
			List2 = [{Meta, Pid} || {Meta, Pid, _Mon} <- List],
			Acc2 = Fun({Class, ObjId}, {Type, List2}, Acc),
			handoff(Next, Acc2, Wait, State);
		[] ->
			handoff(Next, Acc, Wait, State)
	end.


%% @private
wait_for_procs([], _State) ->
	ok;

wait_for_procs([Pid|Rest]=List, #state{timeout=Max}=State) ->
	case is_process_alive(Pid) of
		true ->
			case nklib_util:timestamp() > Max of
				true ->
					?LLOG(warning, "timeout waiting for 'proc' processed", [], State),
					ok;
				false ->
					?LLOG(info, "waiting for ~p 'proc' processes (~p...)", 
					      [length(List), Pid], State),
					timer:sleep(1000*?PROC_RETRY),
					wait_for_procs(List, State)
			end;
		false ->
			wait_for_procs(Rest, State)
	end.



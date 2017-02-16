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

-module(nkdist_ring_event_handler).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_event).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).




%% ===================================================================
%% gen_event behaviour
%% ===================================================================


-record(state, {
	owners = [] :: [{Index::integer(), node()}] 
}).

init([]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    {ok, #state{owners=Owners}}.


handle_event({ring_update, Ring}, #state{owners=Owners}=State) ->
	case riak_core_ring:all_owners(Ring) of
		Owners ->
			{ok, State};
		NewOwners ->
			Diffs = owner_diffs(Owners, NewOwners, []),
			lager:info("New Owners: ~p", [Diffs]),
    		{ok, State#state{owners=NewOwners}}
    end.


handle_call(Msg, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {ok, ok, State}.


handle_info(Info, State) ->
    lager:warning("Module ~p received unexpected info: ~p (~p)", [?MODULE, Info, State]),
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ===================================================================
%% internal
%% ===================================================================

owner_diffs([], [], Acc) ->
	lists:reverse(Acc);

owner_diffs([{Idx, Node1}|Rest1], [{Idx, Node2}|Rest2], Acc) ->
	Acc2 = case Node1==Node2 of
		true ->
			Acc;
		false ->
			[{nkdist_util:idx2pos(Idx), Node1, Node2}|Acc]
	end,
	owner_diffs(Rest1, Rest2, Acc2).










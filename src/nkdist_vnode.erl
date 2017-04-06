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

%% @private Riak Core VNode behaviour
-module(nkdist_vnode).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(riak_core_vnode).

-export([reg/5, unreg/4, unreg_all/3, get/3]).
-export_type([ets_data/0]).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_info/2,
         handle_overload_command/3,
         handle_overload_info/2,
         handle_exit/3,
         set_vnode_forwarding/2]).

-include("nkdist.hrl").
-include_lib("riak_core_ng/include/riak_core_vnode.hrl").

-define(VMASTER, nkdist_vnode_master).

-type vnode() :: {nkdist:vnode_id(), node()}.

-define(ERL_LOW, -1.0e99).
-define(ERL_HIGH, <<255>>).


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
           "NkDIST vnode (~p): "++Txt, [State#state.pos | Args])).



%%%%%%%%%%%%%%%%%%%%%%%%%%%% External %%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @private
-spec reg(vnode(), nkdist:reg_type(), nkdist:obj_class(), 
		  nkdist:obj_id(), nkdist:reg_opts()) ->
	ok | {error, term()}.

reg({Idx, Node}, Type, Class, ObjId, Opts) ->
	Opts2 = maps:merge(#{pid=>self()}, Opts),
	command({Idx, Node}, {reg, Type, Class, ObjId, Opts2}).


%% @private
-spec unreg(vnode(), nkdist:obj_class(), nkdist:obj_id(), nkdist:unreg_opts()) ->
	ok | {error, term()}.

unreg({Idx, Node}, Class, ObjId, Opts) ->
	Pid = maps:get(pid, Opts, self()),
	command({Idx, Node}, {unreg, Class, ObjId, Pid}).


%% @private
-spec unreg_all(vnode(), nkdist:obj_class(), nkdist:obj_id()) ->
	ok | {error, term()}.

unreg_all({Idx, Node}, Class, ObjId) ->
	command({Idx, Node}, {unreg_all, Class, ObjId}).


%% @private
-spec get(vnode(), nkdist:obj_class(), nkdist:obj_id()) ->
	{ok, nkdist:reg_type(), [{nkdist:obj_meta(), pid()}]} |
	{error, term()}.

get({Idx, Node}, Class, ObjId) ->
	command({Idx, Node}, {get, Class, ObjId}).


% %% @private
% %% Sends a synchronous request to the vnode.
% %% If it fails, it will launch an exception
% -spec spawn_command(nkdist:vnode_id(), term()) ->
% 	{ok, term()} | {error, term()}.

% spawn_command({Idx, Node}, Msg) ->
% 	riak_core_vnode_master:sync_spawn_command({Idx, Node}, Msg, ?VMASTER).


%% @private
%% Sends a synchronous request to the vnode.
%% If it fails, it will launch an exception
-spec command(nkdist:vnode_id(), term()) ->
	term() | {error, term()}.

command({Idx, Node}, Msg) ->
	try
		riak_core_vnode_master:sync_command({Idx, Node}, Msg, ?VMASTER)
	catch
		C:E ->
			{error, {C, E}}
	end.


%% @private
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).





%%%%%%%%%%%%%%%%%%%%%%%%%%%% VNode Behaviour %%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type reg() :: {nkdist:obj_meta(), pid(), Mon::reference()}.

-type ets_data() ::
	{{obj, nkdist:obj_class(), nkdist:obj_id()}, nkdist:reg_type(), [reg()]} |
	{{mon, reference()}, nkdist:obj_class(), nkdist:obj_id()}.

-record(state, {
	idx :: chash:index_as_int(),				% vnode's index
	pos :: integer(),
    ets :: ets:tid(),
    handoff_target :: {chash:index_as_int(), node()} | undefined,
    forward :: node() | [{integer(), node()}],
    debug :: boolean()
}).


%% @private
init([Idx]) ->
	Pos = nkdist_util:idx2pos(Idx),
	Debug = nkdist_app:get(debug) == true,
	Ets = ets:new(store, [ordered_set, protected]),
    State = #state{
		idx = Idx,
		pos = Pos,
		ets = Ets,
		debug = Debug
	},
	Workers = nkdist_app:get(vnode_workers),
	FoldWorkerPool = {pool, nkdist_vnode_worker, Workers, [Ets, Pos, Debug]},
	?DEBUG("started (~p, ~p)", [State#state.idx, State#state.ets], State),
    {ok, State, [FoldWorkerPool]}.
		

%% @private
handle_command({reg, reg, Class, ObjId, Opts}, _Send, State) ->
	Reply = insert_single(reg, Class, ObjId, Opts, State),
	{reply, Reply, State};

handle_command({reg, mreg, Class, ObjId, Opts}, _Send, State) ->
	Reply = insert_multi(mreg, Class, ObjId, Opts, State),
	{reply, Reply, State};

handle_command({reg, proc, Class, ObjId, Opts}, _Send, State) ->
	Reply = insert_single(proc, Class, ObjId, Opts, State),
	{reply, Reply, State};

handle_command({reg, master, Class, ObjId, Opts}, _Send, State) ->
	Reply = insert_multi(master, Class, ObjId, Opts, State),
	{reply, Reply, State};

handle_command({reg, leader, Class, ObjId, Opts}, _Send, State) ->
	case nkdist:node_leader() of
		undefined ->
			{reply, {error, no_leader}, State};
		_ ->
			Reply = insert_multi(leader, Class, ObjId, Opts, State),
			{reply, Reply, State}
	end;

handle_command({unreg, Class, ObjId, Pid}, _Send, State) ->
	Reply = do_unreg(Class, ObjId, Pid, State),
	{reply, Reply, State};

handle_command({unreg_all, Class, ObjId}, _Send, State) ->
	case do_get(Class, ObjId, State) of
		not_found ->
			ok;
		{_Type, RegList} ->
			lists:foreach(
				fun({_Meta, Pid, _Mon}) -> do_unreg(Class, ObjId, Pid, State) end,
				RegList)
	end,
	{reply, ok, State};

handle_command({get, Class, ObjId}, _Send, State) ->
	Reply = case do_get(Class, ObjId, State) of
		not_found ->
			{error, obj_not_found};
		{Type, RegList} ->
			{ok, Type, [{Meta, Pid} || {Meta, Pid, _Mon} <- RegList]}
	end,
	{reply, Reply, State};

handle_command(Message, _Sender, State) ->
    ?LLOG(warning, "unhandled command: ~p, ~p", [Message, _Sender], State),
	{reply, {error, unhandled_command}, State}.


%% @private
handle_coverage({get_objs, Class}, _KeySpaces, _Sender, State) ->
	#state{idx=Idx, ets=Ets} = State,
	Data = iter_class(Class, ?ERL_LOW, Ets, []),
	{reply, {vnode, Idx, node(), {done, Data}}, State};

handle_coverage(dump, _KeySpaces, _Sender, #state{ets=Ets, idx=Idx}=State) ->
	{reply, {vnode, Idx, node(), {done, ets:tab2list(Ets)}}, State};

handle_coverage(Cmd, _KeySpaces, _Sender, State) ->
	?LLOG(warning, "unknown coverage: ~p", [Cmd], State),
	{noreply, State}.


%% @private
%% All process registrations except proc are sent to the new vnode inmediately
%% For proc, if it is already at remote node, nothing happens
%% If it is not there, a {must_move, node()} message is sent and we wait for it to stop
%% TODO: what happends if the handoff is cancelled?
handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender, State) -> 
	#state{ets=Ets, handoff_target={_, Node}} = State,
	{async, {handoff, Node, Ets, Fun, Acc0}, Sender, State};

handle_handoff_command({get, Class, ObjId}, _Send, State) ->
	case do_get(Class, ObjId, State) of
		not_found ->
			{forward, State};
		{Type, RegList} ->
			Reply = {ok, Type, [{Meta, Pid} || {Meta, Pid, _Mon} <- RegList]},
			{reply, Reply, State}
	end;

handle_handoff_command(_Cmd, _Sender, State) ->
	{forward, State}.


%% @private
handoff_starting({Type, {Idx, Node}}, State) ->
	?LLOG(info, "handoff (~p) starting to ~p", [Type, Node], State),
    {true, State#state{handoff_target={Idx, Node}}}.


%% @private
handoff_cancelled(State) ->
	?LLOG(notice, "handoff cancelled", [], State),
    {ok, State#state{handoff_target=undefined}}.


%% @private
handoff_finished({_Idx, Node}, State) ->
	?LLOG(info, "handoff finished to ~p", [Node], State),
    {ok, State#state{handoff_target=undefined}}.


%% @private
%% If we reply {error, ...}, the handoff is cancelled, and riak_core will retry it
%% again and again
handle_handoff_data(BinObj, State) ->
	try
		case binary_to_term(BinObj) of
			{{Class, ObjId}, {reg, [{Meta, Pid}]}} ->
				?DEBUG("receiving single ~p: ~p", [reg, {Class, ObjId}], State),
				Opts = #{pid=>Pid, meta=>Meta},
				case insert_single(reg, Class, ObjId, Opts, State) of
					ok ->
						ok;
					{error, {pid_conflict, Pid2}} ->
						send_msg(Pid, {pid_conflict, Pid2});
					{error, {type_conflict, Type2}} ->
						send_msg(Pid, {type_conflict, Type2})
				end,
				{reply, ok, State};
			{{Class, ObjId}, {Type, List}} when Type==mreg; Type==master; Type==leader ->
				?DEBUG("receiving multi ~p: ~p", [Type, {Class, ObjId}], State),
				lists:foreach(
					fun({Meta,Pid}) ->
						Opts = #{pid=>Pid, meta=>Meta},
						case insert_multi(Type, Class, ObjId, Opts, State) of
							ok ->
								ok;
							{error, {type_conflict, Type2}} ->
								send_msg(Pid, {type_conflict, Type2})
						end
					end,
					List),
				{reply, ok, State}
		end
	catch
		C:E ->
			{reply, {error, {{C, E}, erlang:get_stacktrace()}}, State}
	end.


%% @private
encode_handoff_item(Key, Val) ->
	term_to_binary({Key, Val}).


%% @private
is_empty(#state{ets=Ets}=State) ->
	{ets:info(Ets, size)==0, State}.
	

%% @private
delete(State) ->
	?LLOG(info, "deleting", [], State),
    {ok, State}.


%% @private
handle_info({'DOWN', Ref, process, Pid, Reason}, #state{ets=Ets}=State) ->
	case ets:lookup(Ets, {mon, Ref}) of
		[{_, Class, ObjId}] ->
			ok = do_unreg(Class, ObjId, Pid, State),
			{ok, State};
		[] ->
			?LLOG(notice, "unexpected down (~p, ~p)", [Pid, Reason], State),
			{ok, State}
	end;

handle_info(Msg, State) ->
	?LLOG(notice, "unexpected info: ~p", [Msg], State),
	{ok, State}.


%% @private
%% Procs tipically will link to us
handle_exit(Pid, Reason, State) ->
	case Reason of
		normal -> 
			ok;
		_ -> 
			?DEBUG("unhandled EXIT ~p, ~p", [Pid, Reason], State)
	end,
	{noreply, State}.


%% Handling other failures
handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.


%% @private
terminate(normal, _State) ->
	ok;

terminate(Reason, State) ->
	?DEBUG("terminate (~p)", [Reason], State).


%% @private Called from riak core on forwarding state, after the handoff has been
%% completed, but before the new vnode is marked as the owner of the partition
set_vnode_forwarding(Forward, State) ->
    State#state{forward=Forward}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
insert_single(Type, Class, ObjId, Opts, #state{ets=Ets}=State) ->
	Pid = maps:get(pid, Opts),
	Meta = maps:get(meta, Opts, undefined),
	case do_get(Class, ObjId, State) of
		not_found ->
			Mon = monitor(process, Pid),
			Objs = [
				{{obj, Class, ObjId}, Type, [{Meta, Pid, Mon}]},
				{{mon, Mon}, Class, ObjId}
			],
			true = ets:insert(Ets, Objs),
			send_msg(Pid, {vnode_pid, self()}),
			?DEBUG("inserted single: ~p:~p:~p (~p)", 
				   [Type, Class, ObjId, Pid], State),
			case Type of
				proc when node(Pid) /= node() ->
					send_msg(Pid, {must_move, node()});
				_ ->
					ok
			end;
		{Type, [{_OldMeta, Pid, Mon}]} ->
			?DEBUG("updated metadata for: ~p:~p:~p (~p)", 
				   [Type, Class, ObjId, Pid], State),
			true = ets:insert(Ets, {{obj, Class, ObjId}, Type, [{Meta, Pid, Mon}]}),
			ok;
		{Type, [{_Meta, Other, _Mon}]} ->
			case Opts of
				#{replace_pid:=Other} ->
					?DEBUG("removing replaced: ~p:~p:~p (~p)", 
						   [Type, Class, ObjId, Pid], State),
					ok = do_unreg(Class, ObjId, Other, State),
					insert_single(Type, Class, ObjId, Opts, State);
				_ ->
				 	{error, {pid_conflict, Other}}
			end;
		{Type2, _} ->
			{error, {type_conflict, Type2}}
	end.


%% @private
insert_multi(Type, Class, ObjId, Opts, State) ->
	Pid = maps:get(pid, Opts),
	Meta = maps:get(meta, Opts, undefined),
	case do_get(Class, ObjId, State) of
		not_found ->
			do_insert_multi(Type, Class, ObjId, Meta, Pid, [], State),
			ok;
		{Type, List} ->
			do_insert_multi(Type, Class, ObjId, Meta, Pid, List, State),
			ok;
		{Type2, _} ->
			{error, {type_conflict, Type2}}
	end.


%% @private
do_insert_multi(Type, Class, ObjId, Meta, Pid, List, #state{ets=Ets}=State) ->
	?DEBUG("insert multi: ~p, ~p, ~p", [Type, Class, ObjId], State),
	List2 = case lists:keyfind(Pid, 2, List) of
		{_OldMeta, Pid, Mon} ->
			send_msg(Pid, {vnode_pid, self()}),
			lists:keystore(Pid, 2, List, {Meta, Pid, Mon});
		false ->
 			Mon = monitor(process, Pid),
			ets:insert(Ets, {{mon, Mon}, Class, ObjId}),
			List ++ [{Meta, Pid, Mon}]
	end,
	ets:insert(Ets, {{obj, Class, ObjId}, Type, List2}),
	case Type of
		master -> send_master(List2);
		leader -> send_leader(List2);
		_ -> ok
	end.



%% @private
-spec do_get(nkdist:obj_class(), nkdist:obj_id(), #state{}) ->
	{nkdist:reg_type(), [reg()]} | not_found.

do_get(Class, ObjId, #state{ets=Ets}) ->
	case ets:lookup(Ets, {obj, Class, ObjId}) of
		[] ->
			not_found;
		[{_, Type, List}] ->
			{Type, List}
	end.


%% @private
do_unreg(Class, ObjId, Pid, #state{ets=Ets}=State) ->
	case do_get(Class, ObjId, State) of
		not_found ->
			{error, not_found};
		{Type, List} ->
			case lists:keytake(Pid, 2, List) of
				{value, {_Meta, Pid, Mon}, Rest} ->
					demonitor(Mon),
					ets:delete(Ets, {mon, Mon}),
					case Rest of
						[] ->
							ets:delete(Ets, {obj, Class, ObjId});
						_ ->
							ets:insert(Ets, {{obj, Class, ObjId}, Type, Rest}),
							case Type of
								master -> send_master(Rest);
								_ -> ok
							end
					end,
					ok;
				false ->
					{error, not_found}
			end
	end.


%% @private
send_master([{_Meta, Master, _Mon}|_]=List) ->
	lists:foreach(
		fun({_M, Pid, _O}) -> send_msg(Pid, {master, Master}) end,
		List).


%% @private
send_leader(List) ->
	Leader = case nkdist:node_leader() of
		undefined ->
			none;
		Node ->
			case [Pid || {_M, Pid, _O} <- List, node(Pid)==Node] of
				[Pid|_] -> Pid;
				[] -> none
			end
	end,
	lists:foreach(
		fun({_M, Pid, _O}) -> send_msg(Pid, {leader, Leader}) end,
		List).


%% @private
iter_class(Class, Key, Ets, Acc) ->
	case ets:next(Ets, {obj, Class, Key}) of
		{obj, Class, Key2} ->
			iter_class(Class, Key2, Ets, [Key2|Acc]);
		_ ->
			Acc
	end.


%% @private
send_msg(Pid, Msg) ->
	Pid ! {nkdist, Msg},
	ok.





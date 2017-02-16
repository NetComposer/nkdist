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
-include_lib("riak_core/include/riak_core_vnode.hrl").

-define(VMASTER, nkdist_vnode_master).

-type vnode() :: {nkdist:vnode_id(), node()}.

-define(ERL_LOW, -1.0e99).
-define(ERL_HIGH, <<255>>).



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
    ets :: integer(),
    handoff_target :: {chash:index_as_int(), node()},
    forward :: node() | [{integer(), node()}]
}).


%% @private
init([Idx]) ->
    State = #state{
		idx = Idx,
		pos = nkdist_util:idx2pos(Idx),
		ets = ets:new(store, [ordered_set, public])
	},
	Workers = nkdist_app:get(vnode_workers),
	FoldWorkerPool = {pool, nkdist_vnode_worker, Workers, []},
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
	Reply = case do_unreg(Class, ObjId, Pid, State) of
		ok -> ok;
		not_found -> {error, not_found}
	end,
	{reply, Reply, State};

handle_command({unreg_all, Class, ObjId}, _Send, State) ->
	case do_get(Class, ObjId, State) of
		not_found ->
			ok;
		{_Tag, List} ->
			lists:foreach(
				fun({_Meta, Pid, _Mon}) -> do_unreg(Class, ObjId, Pid, State) end,
				List)
	end,
	{reply, ok, State};

handle_command({get, Class, ObjId}, _Send, State) ->
	Reply = case do_get(Class, ObjId, State) of
		not_found ->
			{error, obj_not_found};
		{Tag, List} ->
			{ok, Tag, [{Meta, Pid} || {Meta, Pid, _Mon} <- List]}
	end,
	{reply, Reply, State};

handle_command(Message, _Sender, State) ->
    lager:warning("NkDIST vnode: Unhandled command: ~p, ~p", [Message, _Sender]),
	{reply, {error, unhandled_command}, State}.


%% @private
handle_coverage({get_objs, Class}, _KeySpaces, _Sender, State) ->
	#state{idx=Idx, ets=Ets} = State,
	Data = iter_class(Class, ?ERL_LOW, Ets, []),
	{reply, {vnode, Idx, node(), {done, Data}}, State};

handle_coverage(dump, _KeySpaces, _Sender, #state{ets=Ets, idx=Idx}=State) ->
	{reply, {vnode, Idx, node(), {done, ets:tab2list(Ets)}}, State};

handle_coverage(Cmd, _KeySpaces, _Sender, State) ->
	lager:error("Module ~p unknown coverage: ~p", [?MODULE, Cmd]),
	{noreply, State}.


%% @private
handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender, State) -> 
	#state{ets=Ets, handoff_target={_, Node}} = State,
	{async, {handoff, Node, Ets, Fun, Acc0}, Sender, State};

handle_handoff_command({get, Class, ObjId}, _Send, State) ->
	case do_get(Class, ObjId, State) of
		not_found ->
			{forward, State};
		{Tag, List} ->
			Reply = {ok, Tag, [{Meta, Pid} || {Meta, Pid, _Mon} <- List]},
			{reply, Reply, State}
	end;

handle_handoff_command(_Cmd, _Sender, State) ->
	{forward, State}.


%% @private
handoff_starting({Type, {Idx, Node}}, #state{pos=Pos}=State) ->
	lager:info("NkDIST handoff (~p) starting at ~p to ~p", [Type, Pos, Node]),
    {true, State#state{handoff_target={Idx, Node}}}.


%% @private
handoff_cancelled(#state{pos=Pos}=State) ->
	lager:notice("NkDIST handoff cancelled at ~p", [Pos]),
    {ok, State#state{handoff_target=undefined}}.


%% @private
handoff_finished({_Idx, Node}, #state{pos=Pos}=State) ->
	lager:info("NkDIST handoff finished at ~p to ~p", [Pos, Node]),
    {ok, State#state{handoff_target=undefined}}.


%% @private
%% If we reply {error, ...}, the handoff is cancelled, and riak_core will retry it
%% again and again
handle_handoff_data(BinObj, #state{pos=Pos}=State) ->
	try
		case binary_to_term(BinObj) of
			{{Class, ObjId}, {Type, [{Meta, Pid}]}} when Type==reg; Type==proc ->
				lager:info("Node ~p receiving single ~p: ~p", 
							 [Pos, Type, {Class, ObjId}]),
				Opts = #{pid=>Pid, meta=>Meta},
				case insert_single(Type, Class, ObjId, Opts, State) of
					ok ->
						ok;
					{error, {pid_conflict, Pid2}} ->
						send_msg(Pid, {pid_conflict, Pid2});
					{error, {type_conflict, Type2}} ->
						send_msg(Pid, {type_conflict, Type2})
				end,
				{reply, ok, State};
			{{Class, ObjId}, {Type, List}}	when Type==mreg; Type==master; Type==leader ->
				lager:info("Node ~p receiving multi ~p: ~p", 
							 [Pos, Type, {Class, ObjId}]),
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
delete(#state{pos=Pos}=State) ->
	lager:info("NkDIST vnode ~p deleting", [Pos]),
    {ok, State}.


%% @private
handle_info({'DOWN', Ref, process, Pid, Reason}, #state{ets=Ets, pos=Pos}=State) ->
	case ets:lookup(Ets, {mon, Ref}) of
		[{_, Class, ObjId}] ->
			ok = do_unreg(Class, ObjId, Pid, State),
			{ok, State};
		[] ->
			lager:info("NkDIST vnode ~p unexpected down (~p, ~p)", 
					   [Pos, Pid, Reason]),
			{ok, State}
	end;

handle_info(Msg, State) ->
	lager:warning("Module ~p unexpected info: ~p", [?MODULE, Msg]),
	{ok, State}.


%% @private
%% Procs tipically will link to us
handle_exit(Pid, Reason, #state{pos=Pos}=State) ->
	case Reason of
		normal -> ok;
		_ -> lager:debug("NkDIST vnode ~p unhandled EXIT ~p, ~p", [Pos, Pid, Reason])
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

terminate(Reason, #state{pos=Pos}) ->
	lager:debug("NkDIST vnode ~p terminate (~p)", [Pos, Reason]).


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
			case Type of
				proc when node(Pid) /= node() ->
					send_msg(Pid, {must_move, node()});
				_ ->
					ok
			end;
		{Type, [{_OldMeta, Pid, Mon}]} ->
			true = ets:insert(Ets, {{obj, Class, ObjId}, Type, [{Meta, Pid, Mon}]}),
			ok;
		{Type, [{_Meta, Other, _Mon}]} ->
			case Opts of
				#{replace_pid:=Other} ->
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
do_insert_multi(Type, Class, ObjId, Meta, Pid, List, #state{ets=Ets}) ->
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
			not_found;
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
					not_found
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





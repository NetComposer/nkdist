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

%% @private Riak Core VNode behaviour
-module(nkdist_vnode).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(riak_core_vnode).

-export([get_info/1, find_proc/2, start_proc/4, register/3]).

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
         handle_exit/3,
         set_vnode_forwarding/2]).

-include("nkdist.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").



%%%%%%%%%%%%%%%%%%%%%%%%%%%% External %%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @private
-spec get_info(nkdist:vnode_id()) ->
	{ok, map()}.

get_info({Idx, Node}) ->
	spawn_command({Idx, Node}, get_info).


%% @private
-spec find_proc(nkdist:vnode_id(), nkdist:proc_id()) ->
	{ok, pid()} | {error, not_found}.

find_proc({Idx, Node}, ProcId) ->
	spawn_command({Idx, Node}, {find_proc, ProcId}).


%% @private
-spec start_proc(nkdist:vnode_id(), nkdist:proc_id(), module(), term()) ->
	{ok, pid()} | {error, {already_started, pid()}} | {error, term()}.

start_proc({Idx, Node}, ProcId, CallBack, Args) ->
	spawn_command({Idx, Node}, {start_proc, ProcId, CallBack, Args}).



%% @private
-spec register(nkdist:vnode_id(), atom(), pid()) ->
	{ok, VNode::pid()} | {error, term()}.


register({Idx, Node}, Name, Pid) ->
	command({Idx, Node}, {register, Name, Pid}).


%% @private
%% Sends a synchronous request to the vnode.
%% If it fails, it will launch an exception
-spec spawn_command(nkdist:vnode_id(), term()) ->
	{ok, term()} | {error, term()}.

spawn_command({Idx, Node}, Msg) ->
	riak_core_vnode_master:sync_spawn_command({Idx, Node}, Msg, ?VMASTER).


%% @private
%% Sends a synchronous request to the vnode.
%% If it fails, it will launch an exception
-spec command(nkdist:vnode_id(), term()) ->
	{ok, term()} | {error, term()}.

command({Idx, Node}, Msg) ->
	riak_core_vnode_master:sync_command({Idx, Node}, Msg, ?VMASTER).


%% @private
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).





%%%%%%%%%%%%%%%%%%%%%%%%%%%% VNode Behaviour %%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(proc, {
	callback :: module(),
	pid :: pid(),
	mon :: reference()
}).


-record(state, {
	idx :: chash:index_as_int(),				% vnode's index
	pos :: integer(),
	procs :: #{nkdist:proc_id() => #proc{}},
	masters :: #{atom() => [pid()]},			% first in row is master
	pids :: #{pid() => {proc, nkdist:proc_id()} | {master, atom()}},
    handoff_target :: {chash:index_as_int(), node()},
    forward :: node() | [{integer(), node()}]
}).


%% @private
init([Idx]) ->
    State = #state{
		idx = Idx,
		pos = nkdist_util:idx2pos(Idx),
		procs = #{},
		pids = #{},
		masters = #{}
	},
	Workers = application:get_env(?APP, vnode_workers, 1),
	FoldWorkerPool = {pool, nkdist_vnode_worker, Workers, []},
    {ok, State, [FoldWorkerPool]}.
		

%% @private
handle_command(get_info, _Sender, #state{idx=Idx, pos=Pos, procs=Procs}=State) ->
	Reply = #{
		pid => self(),
		idx => Idx,
		pos => Pos,
		procs => Procs
	},
	{reply, {ok, Reply}, State};


handle_command({find_proc, ProcId}, _Sender, #state{procs=Procs, idx=Idx, pos=Pos}=State) ->
	lager:debug("FIND ~p at ~p {~p, ~p}", [ProcId, Pos, Idx, node()]),
	case maps:get(ProcId, Procs, undefined) of
		#proc{pid=Pid} ->
			{reply, {ok, Pid}, State};
		undefined ->
			{reply, {error, not_found}, State}
	end;

handle_command({start_proc, ProcId, CallBack, Args}, _Sender, State) ->
	#state{procs=Procs, idx=Idx, pos=Pos} = State,
	case maps:get(ProcId, Procs, undefined) of
		#proc{pid=Pid} ->
			{reply, {error, {already_started, Pid}}, State};
		undefined ->
			lager:debug("START ~p at ~p {~p, ~p}", [ProcId, Pos, Idx, node()]),
			try 
				case CallBack:start(ProcId, Args) of
					{ok, Pid} ->
						State1 = started_proc(ProcId, CallBack, Pid, State),
						{reply, {ok, Pid}, State1};
					{error, Error} ->
						{reply, {error, Error}, State}
				end
			catch
				C:E->
            		{reply, {error, {{C, E}, erlang:get_stacktrace()}}, State}
           	end
    end;

handle_command({register, Name, Pid}, _Send, State) ->
	State1 = do_register(Name, Pid, State),
	{reply, {ok, self()}, State1};

handle_command(Message, _Sender, State) ->
    lager:warning("NkDIST VNode: Unhandled command: ~p, ~p", [Message, _Sender]),
	{reply, {error, unhandled_command}, State}.


%% @private
handle_coverage(get_procs, _KeySpaces, _Sender, State) ->
	#state{procs=Procs, idx=Idx} = State,
	Data = lists:map(
		fun({ProcId, #proc{callback=CallBack, pid=Pid}}) ->
			{ProcId, CallBack, Pid}
		end,
		maps:to_list(Procs)),
	{reply, {vnode, Idx, node(), {done, Data}}, State};

handle_coverage({get_procs, CallBack}, _KeySpaces, _Sender, State) ->
	#state{procs=Procs, idx=Idx} = State,
	Data = lists:filtermap(
		fun({ProcId, #proc{callback=C, pid=Pid}}) ->
			case C of
				CallBack -> {true, {ProcId, Pid}};
				_ -> false
			end
		end,
		maps:to_list(Procs)),
	{reply, {vnode, Idx, node(), {done, Data}}, State};


handle_coverage(Cmd, _KeySpaces, _Sender, State) ->
	lager:error("Module ~p unknown coverage: ~p", [?MODULE, Cmd]),
	{noreply, State}.


%% @private
handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender, State) -> 
	#state{masters=Masters, procs=Procs} = State,
	MastersData = maps:to_list(Masters),
	ProcsData = [
		{ProcId, CallBack, Pid} ||
		{ProcId, #proc{callback=CallBack, pid=Pid}} <- maps:to_list(Procs)
	],
	{async, {handoff, MastersData, ProcsData, Fun, Acc0}, Sender, State};

handle_handoff_command({find_proc, ProcId}, _Sender, #state{procs=Procs}=State) ->
	case maps:get(ProcId, Procs, undefined) of
		#proc{pid=Pid} ->
			{reply, {ok, Pid}, State};
		undefined ->
			{forwward, State}
	end;

handle_handoff_command(Term, _Sender, State) when
		element(1, Term)==register; element(1, Term)==start_proc ->
	{forward, State};

% Process rest of operations locally
handle_handoff_command(Cmd, Sender, State) ->
	lager:info("Handoff command ~p at ~p", [Cmd, State#state.pos]),
	handle_command(Cmd, Sender, State).


%% @private
handoff_starting({Type, {Idx, Node}}, #state{pos=Pos}=State) ->
	lager:info("Handoff (~p) starting at ~p to ~p", [Type, Pos, Node]),
    {true, State#state{handoff_target={Idx, Node}}}.


%% @private
handoff_cancelled(#state{masters=Masters, pos=Pos}=State) ->
	lager:notice("Handoff cancelled at ~p", [Pos]),
	lists:foreach(
		fun({Name, Pids}) -> send_master(Name, Pids) end,
		maps:to_list(Masters)),
    {ok, State#state{handoff_target=undefined}}.


%% @private
handoff_finished({_Idx, Node}, #state{pos=Pos}=State) ->
	lager:info("Handoff finished at ~p to ~p", [Pos, Node]),
    {ok, State#state{handoff_target=undefined}}.


%% @private
%% If we reply {error, ...}, the handoff is cancelled, and riak_core will retry it
%% again and again
handle_handoff_data(BinObj, #state{procs=Procs}=State) ->
	try
		case binary_to_term(zlib:unzip(BinObj)) of
			{{proc, ProcId}, {CallBack, OldPid}} ->
				case maps:get(ProcId, Procs, undefined) of
					undefined ->
						lager:debug("Calling START AND JOIN"),
						case CallBack:start_and_join(ProcId, OldPid) of
							{ok, NewPid} ->
								State1 = started_proc(ProcId, CallBack, NewPid, State),
								{reply, ok, State1};
							{error, Error} ->
					 			{reply, {error, Error}, State}
					 	end;
					#proc{pid=NewPid} ->
						lager:debug("Calling JOIN"),
						case CallBack:join(NewPid, OldPid) of
							ok ->
								{reply, ok, State};
							{error, Error} ->
					 			{reply, {error, Error}, State}
					 	end
				end;
			{{masters, Name}, Pids} ->
				State1 = lists:fold(
					fun(Pid, Acc) -> do_register(Name, Pid, Acc) end,
					State,
					Pids),
				{reply, ok, State1}
		end
	catch
		C:E ->
			{reply, {error, {{C, E}, erlang:get_stacktrace()}}, State}
	end.


%% @private
encode_handoff_item(Key, Val) ->
	zlib:zip(term_to_binary({Key, Val})).


%% @private
is_empty(#state{pos=Pos, procs=Procs}=State) ->
	IsEmpty = maps:size(Procs)==0,
	lager:info("VNode ~p is empty = ~p", [Pos, IsEmpty]),
	{IsEmpty, State}.
	

%% @private
delete(#state{pos=Pos}=State) ->
	lager:info("VNode ~p deleting", [Pos]),
    {ok, State}.


%% @private
handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
	#state{procs=Procs, pids=Pids, masters=Masters, pos=Pos} = State,
	case maps:get(Pid, Pids, undefined) of
		undefined ->
			case Reason of
				normal -> 
					ok;
				_ -> 
					lager:notice("VNode ~p unexpected DOWN: ~p (~p)", 
								 [Pos, Pid, Reason])
			end,
			{ok, State};
		{proc, ProcId} ->
			lager:info("VNODE ProcID '~p' DOWN", [ProcId]),
			Procs1 = maps:remove(ProcId, Procs),
			Pids1 = maps:remove(Pid, Pids),
			{ok, State#state{procs=Procs1, pids=Pids1}};
		{master, Name} ->
			lager:info("VNODE Master '~p' DOWN (~p, ~p)", [Name, Pid, Reason]),
			MasterPids = maps:get(Name, Masters),
			case MasterPids -- [Pid] of
				[] ->
					Masters1 = maps:remove(Name, Masters),
					Pids1 = maps:remove(Pid, Pids),
					{ok, State#state{masters=Masters1, pids=Pids1}};
				MasterPids1 ->
					case State#state.handoff_target of
						undefined -> send_master(Name, MasterPids1);
						_ -> ok
					end,
					Masters1 = maps:put(Name, MasterPids1, Masters),
					Pids1 = maps:remove(Pid, Pids),
					{ok, State#state{masters=Masters1, pids=Pids1}}
			end
	end;

handle_info(Msg, State) ->
	lager:warning("Module ~p unexpected info: ~p", [?MODULE, Msg]),
	{ok, State}.


%% @private
%% Procs tipically will link to us
handle_exit(Pid, Reason, #state{pos=Pos}=State) ->
	case Reason of
		normal -> ok;
		_ -> lager:debug("VNode ~p: Unhandled EXIT ~p, ~p", [Pos, Pid, Reason])
	end,
	{noreply, State}.


%% @private
terminate(normal, _State) ->
	ok;

terminate(Reason, #state{pos=Pos}) ->
	lager:debug("VNode ~p terminate: ~p", [Pos, Reason]).


%% @private Called from riak core on forwarding state, after the handoff has been
%% completed, but before the new vnode is marked as the owner of the partition
set_vnode_forwarding(Forward, State) ->
    State#state{forward=Forward}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @private
do_register(Name, Pid, #state{masters=Masters, pids=Pids}=State) ->
	MasterPids = maps:get(Name, Masters, []),
	case lists:member(Pid, MasterPids) of
		true ->
			State;
		false ->
			monitor(process, Pid),
			MasterPids1 = MasterPids ++ [Pid],
			send_master(Name, MasterPids1),
			Masters1 = maps:put(Name, MasterPids1, Masters),
			Pids1 = maps:put(Pid, {master, Name}, Pids),
			State#state{masters=Masters1, pids=Pids1}
	end.


%% @private
started_proc(ProcId, CallBack, Pid, #state{procs=Procs, pids=Pids}=State) ->
	Proc = #proc{callback=CallBack, pid=Pid, mon=monitor(process, Pid)},
	Procs1 = maps:put(ProcId, Proc, Procs),
	Pids1 =  maps:put(Pid, {proc, ProcId}, Pids),
	State#state{procs=Procs1, pids=Pids1}.


%% @private Elects master as first pid on this node
send_master(Name, [Master|_]=Pids) ->
	lists:foreach(fun(Pid) -> Pid ! {nkdist_master, Name, Master} end, Pids).
	
% %% @private Say no master to everyone
% send_no_master(Name, Pids) ->
% 	lists:foreach(fun(Pid) -> Pid ! {nkdist_master, Name, undefined} end, Pids).






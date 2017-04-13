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

-module(nkdist_reg).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).


-export([find/2, find/3, reg/3, reg/4, link/3]).
-export([start_link/0, init/1, terminate/2, code_change/3, handle_call/3,
	handle_cast/2, handle_info/2]).

-define(LLOG(Type, Txt, Args), lager:Type("NkDIST REG "++Txt, Args)).



%% ===================================================================
%% Public functions
%% ===================================================================


%% @doc Finds a key in cache, if not found finds in vnode and store
-spec find(nkdist:obj_class(), nkdist:obj_id()) ->
	{ok, nkdist:obj_meta(), pid()} | not_found.

find(Class, ObjId) ->
	find(Class, ObjId, #{}).


%% @doc Finds a key in cache, if not found finds in vnode and store
-spec find(nkdist:obj_class(), nkdist:obj_id(), nkdist:get_opts()) ->
	{ok, nkdist:obj_meta(), pid()} | not_found.

find(Class, ObjId, Opts) ->
	case lookup_reg(ObjId) of
		not_found ->
			case nkdist:get(Class, ObjId, Opts) of
				{ok, proc, [{Meta, Pid}]} ->
					gen_server:cast(?MODULE, {put, ObjId, Meta, Pid}),
					{ok, Pid};
                {ok, reg, [{Meta, Pid}]} ->
                    gen_server:cast(?MODULE, {put, ObjId, Meta, Pid}),
                    {ok, Pid};
				{ok, _, _} ->
					{error, invalid_reg};
				{error, Error} ->
					{error, Error}
			end;
		{Meta, Pid} ->
			{ok, Meta, Pid}
	end.



%% @doc Stores a new registration and updates cache
-spec reg(nkdist:reg_type(), nkdist:obj_class(), nkdist:obj_id()) ->
	ok | {error, term()}.

reg(Type, Class, ObjId) ->
	reg(Type, Class, ObjId, #{}).


%% @doc Stores a new registration and updates cache
-spec reg(nkdist:reg_type(), nkdist:obj_class(), nkdist:obj_id(), nkdist:reg_opts()) ->
	ok | {error, term()}.

reg(Type, Class, ObjId, Opts) ->
	case nkdist:register(Type, Class, ObjId, Opts) of
		ok ->
			Meta = maps:get(meta, Opts, undefined),
			gen_server:cast(?MODULE, {put, ObjId, Meta, self()});
		{error, Error} ->
			{error, Error}
	end.


%% @doc
-spec link(nkdist:obj_class(), nkdist:obj_id(), term()) ->
	ok | {error, term()}.

link(Class, DestObjId, Tag) ->
	case find(Class, DestObjId) of
		{ok, _, DestPid} ->
			Msg = {link, Tag, self(), DestObjId, DestPid},
			case node(DestPid) of
                Node when Node==node() ->
                    gen_server:cast(?MODULE, Msg);
				Node ->
					gen_server:abcast([Node], ?MODULE, Msg),
                    ok
			end;
		{error, Error} ->
			{error, Error}
	end.


%% ===================================================================
%% gen_server
%% ===================================================================

%% ETS:
%% - {{reg, nkdist:obj_id()}, Meta::term(), pid()}
%% - {{link, Tag::term(), Dest::nkdist:obj_id()}, Orig::pid(), Dest::pid()}
%% - {{pid, pid()}, Mon::reference(), [{reg, nkdist:obj_id()}|{link, Tag, Dest}]}


-record(state, {
}).

%% @private
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% @private
-spec init(term()) ->
	{ok, #state{}}.

init([]) ->
	process_flag(trap_exit, true),
	ets:new(?MODULE, [protected, named_table]),
	{ok, #state{}}.


%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
	{reply, term(), #state{}} | {noreply, #state{}} | {stop, normal, ok, #state{}}.

handle_call(Msg, _From, State) ->
	lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
	{noreply, State}.


%% @private
-spec handle_cast(term(), #state{}) ->
	{noreply, #state{}}.

handle_cast({put, ObjId, Meta, Pid}, State) ->
	insert_reg(ObjId, Meta, Pid),
	{noreply, State};

handle_cast({link, Tag, OrigPid, DestObjId, DestPid}, State) ->
	insert_link(Tag, OrigPid, DestObjId, DestPid),
	{noreply, State};

handle_cast(Msg, State) ->
	lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
	{noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
	{noreply, #state{}}.

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
	case lookup_pid(Pid) of
		not_found ->
			?LLOG(notice, "received unexpected DOWN: ~p", [Pid]);
		{Ref, Items} ->
			unregister_pid(Items, Pid)
	end,
	{noreply, State};

handle_info({'EXIT', _Pid, _Reason}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	lager:warning("Module ~p received unexpected cast ~p", [?MODULE, Info]),
	{noreply, State}.


%% @private
-spec code_change(term(), #state{}, term()) ->
	{ok, #state{}}.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


%% @private
-spec terminate(term(), #state{}) ->
	ok.

terminate(_Reason, _State) ->
	ok.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
lookup_reg(ObjId) ->
	case ets:lookup(?MODULE, {key, ObjId}) of
		[] -> not_found;
		[{_, Meta, Pid}] -> {Meta, Pid}
	end.


%% @private
store_reg(ObjId, Meta, Pid) ->
	ets:insert(?MODULE, {{key, ObjId}, Meta, Pid}).


%% @private
delete_reg(ObjId) ->
	ets:delete(?MODULE, {key, ObjId}).


%% @private
lookup_pid(Pid) ->
	case ets:lookup(?MODULE, {pid, Pid}) of
		[] -> not_found;
		[{_, Ref, Items}] -> {Ref, Items}
	end.

%% @private
store_pid(Pid, Ref, Items) ->
	ets:insert(?MODULE, {{pid, Pid}, Ref, Items}).


%% @private
delete_pid(Pid) ->
	ets:delete(?MODULE, {pid, Pid}).


%% @private
lookup_link(Tag, DestObjId) ->
	case ets:lookup(?MODULE, {link, Tag, DestObjId}) of
		[] -> not_found;
		[{_, OrigPid, DestPid}] -> {OrigPid, DestPid}
	end.


%% @private
store_link(Tag, OrigPid, Dest, DestPid) ->
	ets:insert(?MODULE, {{link, Tag, Dest}, OrigPid, DestPid}).


%% @private
delete_link(Tag, Dest) ->
	ets:delete(?MODULE, {link, Tag, Dest}).


%% @private
insert_reg(ObjId, Meta, Pid) ->
	case lookup_reg(ObjId) of
		not_found ->
			store_reg(ObjId, Meta, Pid);
		{Meta, Pid} ->
			ok;
		{_OldMeta, Pid} ->
			store_reg(ObjId, Meta, Pid);
		{_OldMeta, OldPid} ->
			send_msg(OldPid, {new_registered_process, ObjId, OldPid}),
			store_reg(ObjId, Meta, Pid)
	end,
	insert_pid({reg, ObjId}, Pid).


%% @private
insert_link(Tag, OrigPid, DestObjId, DestPid) ->
	case lookup_link(Tag, DestObjId) of
		{OrigPid, DestPid} ->
			ok;
		not_found ->
			store_link(Tag, OrigPid, DestObjId, DestPid),
			insert_pid({link, Tag, DestObjId}, OrigPid),
			insert_pid({link, Tag, DestObjId}, DestPid),
			send_msg(DestPid, {new_link, Tag});
		{_Pid1, _Pid2} ->
			?LLOG(warning, "received link ~p ~p with new pids", [Tag, DestObjId])
	end.


%% @private
insert_pid(Id, Pid) ->
	case lookup_pid(Pid) of
		not_found ->
			Ref = monitor(process, Pid),
			store_pid(Pid, Ref, [Id]);
		{Ref, Ids} ->
			case lists:member(Id, Ids) of
				true ->
					ok;
				false ->
					store_pid(Pid, Ref, [Id|Ids])
			end
	end.


%% @private
unregister_pid(Items, Pid) ->
	delete_pid(Pid),
	unregister_items(Items, Pid).

%% @private
unregister_pid_item(Item, Pid) ->
	{Ref, Items} = lookup_pid(Pid),
	store_pid(Pid, Ref, Items -- [Item]).



%% @private
unregister_items([], _Pid) ->
	ok;

unregister_items([{reg, ObjId}|Rest], Pid) ->
	delete_reg(ObjId),
	unregister_items(Rest, Pid);

unregister_items([{link, Tag, DestObjId}|Rest], Pid) ->
	case lookup_link(Tag, DestObjId) of
		{Pid, DestPid} ->
			%% The orig pid has fallen, notify Dest
			unregister_pid_item({link, Tag, DestObjId}, DestPid),
			send_msg(DestPid, {link_fallen, Tag});
		{OrigPid, Pid} ->
			%% The dest pid has fallen
			unregister_pid_item({link, Tag, DestObjId}, OrigPid)
	end,
	delete_link(Tag, DestObjId),
	unregister_items(Rest, Pid).


%% @private
send_msg(Pid, Msg) ->
	?LLOG(notice, "sending msg to ~p: ~p", [Pid, Msg]),
	Pid ! {?MODULE, Msg}.

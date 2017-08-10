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

-export([find/2, find/3, register/3, register/4, unregister/2, unregister/3]).
-export([link/3, link_pid/2, link_pid/3, unlink/3, unlink_pid/2, unlink_pid/3]).
-export([reserve/2, unreserve/2]).
-export([update_pid/2]).
-export([start_link/0, init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export([test/0]).
-export_type([msg/0]).


-define(DEBUG(Txt, Args, State),
    case State#state.debug of
        true -> ?LLOG(debug, Txt, Args);
        _ -> ok
    end).

-define(LLOG(Type, Txt, Args), lager:Type("NkDIST REG "++Txt, Args)).

-define(SYNC_CALL_TIME, 30000).

%% ===================================================================
%% Types
%% ===================================================================

-type tag() :: term().

-type reg_opts() ::
    nkdist:reg_opts() | #{sync => boolean()}.

-type unreg_opts() ::
    nkdist:unreg_opts().



-type msg() ::
    {received_link, tag()} |        % A new link has been received
    {removed_link, tag()} |         % An existing link has been removed
    {received_link_down, tag()} |   % A received link has fallen
    {sent_link_down, tag()} |       % A link we sent has failed
    {updated_reg_pid, pid()}.       % A new process has been registered with the same name
                                    % It will never happen for 'proc' types


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
	case ets_lookup_reg(Class, ObjId) of
		not_found ->
			case nkdist:get(Class, ObjId, Opts) of
				{ok, proc, [{Meta, Pid}]} ->
					gen_server:cast(?MODULE, {put, Class, ObjId, Meta, Pid}),
					{ok, Pid};
                {ok, reg, [{Meta, Pid}]} ->
                    gen_server:cast(?MODULE, {put, Class, ObjId, Meta, Pid}),
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
-spec register(nkdist:reg_type(), nkdist:obj_class(), nkdist:obj_id()) ->
	ok | {error, term()}.

register(Type, Class, ObjId) ->
	register(Type, Class, ObjId, #{}).


%% @doc Stores a new registration and updates cache
-spec register(nkdist:reg_type(), nkdist:obj_class(), nkdist:obj_id(), reg_opts()) ->
	ok | {error, {pid_conflict, pid()}|term()}.

register(Type, Class, ObjId, Opts) ->
	case nkdist:register(Type, Class, ObjId, Opts) of
		ok ->
			Meta = maps:get(meta, Opts, undefined),
            Pid = maps:get(pid, Opts, self()),
            Msg = {put, Class, ObjId, Meta, Pid},
            case maps:get(sync, Opts, false) of
                true ->
                    gen_server:call(?MODULE, Msg, ?SYNC_CALL_TIME);
                false ->
			        gen_server:cast(?MODULE, Msg)
            end;
		{error, Error} ->
			{error, Error}
	end.


%% @doc Removes a registration and updates cache
-spec unregister(nkdist:obj_class(), nkdist:obj_id()) ->
    ok | {error, {pid_conflict, pid()}|term()}.

unregister(Class, ObjId) ->
    nkdist:unregister(Class, ObjId, #{}).


%% @doc Removes a registration and updates cache
-spec unregister(nkdist:obj_class(), nkdist:obj_id(), unreg_opts()) ->
    ok | {error, {pid_conflict, pid()}|term()}.

unregister(Class, ObjId, Opts) ->
    nkdist:unregister(Class, ObjId, Opts),
    Pid = maps:get(pid, Opts, self()),
    Msg = {rm, Class, ObjId, Pid},
    gen_server:cast(?MODULE, Msg).


%% @doc Links current process to another object
%% DestId will be resolved, and:
%% - at the local node, a link will be inserted monitoring both pids
%% - at the node of DestPid, another link will be inserted monitoring also both
%% - remote node sends {received_link, Tag} to DestId
%% - if caller dies, local node removes the link, remote node sends {received_link_down, Tag}
%% - if dest dies, local node sends {sent_link_down, Tag}, remote node removes the link
%% You can update the pid of caller or dest (a broadcast will be sent when calling update_pid/2)
%% If a process is registered using {replace_pid=>OldPid}, the vnode will call update_pid/2 by itself
-spec link(nkdist:obj_class(), nkdist:obj_id(), tag()) ->
	ok | {error, term()}.

link(Class, DestId, Tag) ->
    case find(Class, DestId) of
        {ok, _, DestPid} when is_pid(DestPid) ->
            link_pid(self(), DestPid, Tag);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Links directly to a remote pid
-spec link_pid(pid(), tag()) ->
    ok | {error, term()}.

link_pid(DestPid, Tag) when is_pid(DestPid) ->
    link_pid(self(), DestPid, Tag).


%% @doc Links directly two pids
-spec link_pid(pid(), pid(), tag()) ->
    ok | {error, term()}.

link_pid(OrigPid, DestPid, Tag) when is_pid(OrigPid), is_pid(DestPid) ->
    gen_server:call(?MODULE, {link, OrigPid, DestPid, Tag}, ?SYNC_CALL_TIME).


%% @doc Removes a link, using DestId and tag
-spec unlink(nkdist:obj_class(), nkdist:obj_id(), tag()) ->
    ok | {error, term()}.

unlink(Class, DestId, Tag) ->
    case find(Class, DestId) of
        {ok, _, DestPid} when is_pid(DestPid) ->
            unlink_pid(self(), DestPid, Tag);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Removes a link to a remote pid directly
-spec unlink_pid(pid(), tag()) ->
    ok | {error, term()}.

unlink_pid(DestPid, Tag) when is_pid(DestPid) ->
    unlink(self(), DestPid, Tag).


%% @doc Removes a link with two pids directly
-spec unlink_pid(pid(), pid(), tag()) ->
    ok | {error, term()}.

unlink_pid(OrigPid, DestPid, Tag) when is_pid(OrigPid), is_pid(DestPid) ->
    gen_server:cast(?MODULE, {unlink, OrigPid, DestPid, Tag}).


%% @doc "Reserves" an object registration at this node
%% Any other reservation will fail
%% Caller process must call remove_reserve/2 (or die) to remove the reservation
-spec reserve(nkdist:obj_class(), nkdist:obj_id()) ->
    ok | {error, {already_reserved, pid()}|term()}.

reserve(Class, ObjId) ->
    gen_server:call(?MODULE, {reserve, Class, ObjId}).


%% @doc Removes a reservation
-spec unreserve(nkdist:obj_class(), nkdist:obj_id()) ->
    ok.

unreserve(Class, ObjId) ->
    gen_server:cast(?MODULE, {unreserve, Class, ObjId}).


%% @private Received from nkdist_vnode when a process is registered with an update pid
update_pid(OldPid, NewPid) ->
    gen_server:abcast(?MODULE, {update_pid, OldPid, NewPid}).


%% ===================================================================
%% gen_server
%% ===================================================================

%% ETS:
%% - {{reg, nkdist:obj_class(), nkdist:obj_id()}, Meta::term(), pid()}
%% - {{reserve, nkdist:obj_class(), nkdist:obj_id()}, pid()}
%% - {{pid, pid()}, Mon::reference(), [
%%      {reg, nkdist:obj_class(), nkdist:obj_id()} |
%%      {reserve, nkdist:obj_class(), nkdist:obj_id()} |
%%      {link_to, pid(), Tag} | {link_from, pid(), Tag}]}


-record(state, {
    debug :: boolean()
}).

%% @private
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% @private
-spec init(term()) ->
	{ok, #state{}}.

init([]) ->
	process_flag(trap_exit, true),
	ets:new(?MODULE, [protected, named_table, {read_concurrency, true}]),
    Debug = nkdist_app:get(debug) == true,
    {ok, #state{debug=Debug}}.


%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
	{reply, term(), #state{}} | {noreply, #state{}} | {stop, normal, ok, #state{}}.

handle_call({put, Class, ObjId, Meta, Pid}, _From, State) ->
    insert_reg(Class, ObjId, Meta, Pid, State),
    {reply, ok, State};


%% The node receiving the link monitors both sides with 'orig'
%% The node at DestPid monitor both with 'dest' and sends {received_link, Tag} to DestPid
%% If we (the OrigPid) die, we do nothing


handle_call({link, OrigPid, DestPid, Tag}, _From, State) ->
    To = insert_item_pid({link_to, DestPid, Tag, orig}, OrigPid),
    Dest = insert_item_pid({link_from, OrigPid, Tag, orig}, DestPid),
    case To==not_stored andalso Dest==not_stored of
        true ->
            % It is a re-registration with the same data
            ok;
        false ->
            gen_server:cast({?MODULE, node(DestPid)}, {link_dest, OrigPid, DestPid, Tag})
    end,
    {reply, ok, State};

handle_call({reserve, Class, ObjId}, From, State) ->
    insert_reserve(Class, ObjId, From),
    {noreply, State};

handle_call(Msg, _From, State) ->
	lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
	{noreply, State}.


%% @private
-spec handle_cast(term(), #state{}) ->
	{noreply, #state{}}.

handle_cast({put, Class, ObjId, Meta, Pid}, State) ->
	insert_reg(Class, ObjId, Meta, Pid, State),
	{noreply, State};

handle_cast({rm, Class, ObjId, Pid}, State) ->
    remove_reg(Class, ObjId, Pid),
    {noreply, State};

handle_cast({link_dest, OrigPid, DestPid, Tag}, State) ->
    insert_item_pid({link_to, OrigPid, Tag, dest}, DestPid),
    insert_item_pid({link_from, DestPid, Tag, dest}, OrigPid),
    send_msg(DestPid, {received_link, Tag}, State),
    {noreply, State};

handle_cast({unlink, OrigPid, DestPid, Tag}, State) ->
    gen_server:cast({?MODULE, node(DestPid)}, {unlink_dest, OrigPid, DestPid, Tag}),
    remove_item_pid({link_to, DestPid, Tag, orig}, OrigPid),
    remove_item_pid({link_from, OrigPid, Tag, orig}, DestPid),
    {noreply, State};

handle_cast({unlink_dest, OrigPid, DestPid, Tag}, State) ->
    case
        remove_item_pid({link_to, OrigPid, Tag, dest}, DestPid) == removed andalso
        remove_item_pid({link_from, DestPid, Tag, dest}, OrigPid) == removed
    of
        true ->
            send_msg(DestPid, {removed_link, Tag}, State);
        false ->
            ok
    end,
    {noreply, State};

handle_cast({unreserve, Class, ObjId}, State) ->
    remove_reserve(Class, ObjId),
    {noreply, State};

handle_cast({update_pid, OldPid, NewPid}, State) ->
    case ets_lookup_pid(OldPid) of
        not_found ->
            ok;
        {Ref, Items} ->
            update_items_pid(Items, OldPid, NewPid, State),
            ets_delete_pid(OldPid, Ref)
    end,
    {noreply, State};

handle_cast(Msg, State) ->
	lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
	{noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
	{noreply, #state{}}.

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
	case ets_lookup_pid(Pid) of
		not_found ->
			?LLOG(info, "received unexpected DOWN: ~p", [Pid]);
		{Ref, Items} ->
            remove_items_pid(Items, Pid, State),
            ets_delete_pid(Pid, Ref)

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
insert_reg(Class, ObjId, Meta, Pid, State) ->
	case ets_lookup_reg(Class, ObjId) of
		not_found ->
			ets_store_reg(Class, ObjId, Meta, Pid);
		{Meta, Pid} ->
			ok;
		{_OldMeta, Pid} ->
			ets_store_reg(Class, ObjId, Meta, Pid);
		{_OldMeta, OldPid} ->
			send_msg(OldPid, {updated_reg_pid, Pid}, State),
			ets_store_reg(Class, ObjId, Meta, Pid)
	end,
	insert_item_pid({reg, Class, ObjId}, Pid).


%% @private
remove_reg(Class, ObjId, Pid) ->
    ets_delete_reg(Class, ObjId),
    remove_item_pid({reg, Class, ObjId}, Pid).


%% @private
insert_reserve(Class, ObjId, {Pid, _Ref}=From) ->
    case ets_lookup_reserve(Class, ObjId) of
        not_found ->
            ets_store_reserve(Class, ObjId, Pid),
            insert_item_pid({reserve, Class, ObjId}, Pid),
            gen_server:reply(From, ok);
        Pid2 when is_pid(Pid2) ->
            gen_server:reply(From, {error, {already_reserved, Pid2}})
    end.


%% @private
remove_reserve(Class, ObjId) ->
    case ets_lookup_reserve(Class, ObjId) of
        not_found ->
            ok;
        Pid when is_pid(Pid) ->
            remove_item_pid({reserve, Class, ObjId}, Pid),
            ets_delete_reserve(Class, ObjId)
    end.


%% @private
insert_item_pid(Item, Pid) ->
	case ets_lookup_pid(Pid) of
		not_found ->
			Ref = monitor(process, Pid),
			ets_store_pid(Pid, Ref, [Item]),
            stored;
		{Ref, Items} ->
			case lists:member(Item, Items) of
				true ->
					not_stored;
				false ->
					ets_store_pid(Pid, Ref, [Item|Items]),
                    stored
			end
	end.


%% @private
remove_item_pid(Item, Pid) ->
    case ets_lookup_pid(Pid) of
        {Ref, [Item]} ->
            ets_delete_pid(Pid, Ref),
            removed;
        {Ref, Items} ->
            case lists:member(Item, Items) of
                true ->
                    ets_store_pid(Pid, Ref, Items--[Item]),
                    removed;
                false ->
                    not_removed
            end;
        not_found ->
            not_removed
    end.

%% @private
remove_items_pid([], _Pid, _State) ->
	ok;

remove_items_pid([Item|Rest], Pid, State) ->
    case Item of
        {reg, Class, ObjId} ->
            ets_delete_reg(Class, ObjId);
        {reserve, Class, ObjId} ->
            ets_delete_reserve(Class, ObjId);
        {link_to, DestPid, Tag, orig} ->
            % We are at Orig's node and OrigPid has fallen.
            % It makes no sense sending anything since the remote node, if alive, will detect it
            remove_item_pid({link_from, Pid, Tag, orig}, DestPid);
        {link_from, OrigPid, Tag, orig} ->
            %% We are at Orig's node and DestPid has fallen.
            %% We notify OrigPid with sent_link_down
            send_msg(OrigPid, {sent_link_down, Tag}, State),
            remove_item_pid({link_to, Pid, Tag, orig}, OrigPid);
        {link_to, OrigPid, Tag, dest} ->
            % We are at Dest's node and DestPid has fallen
            % It makes no sense sending anything
            remove_item_pid({link_from, Pid, Tag, dest}, OrigPid);
        {link_from, DestPid, Tag, dest} ->
            % We are at Dest's node and OrigPid has fallen.
            % We notify DestPid wi
            send_msg(DestPid, {received_link_down, Tag}, State),
            remove_item_pid({link_to, Pid, Tag, dest}, DestPid)
    end,
	remove_items_pid(Rest, Pid, State).


%% @private
update_items_pid([], _OldPid, _NewPid, _State) ->
    ok;

update_items_pid([Item|Rest], OldPid, NewPid, State) ->
    case Item of
        {reg, Class, ObjId} ->
            ets_delete_reg(Class, ObjId);
        {reserve, Class, ObjId} ->
            ets_delete_reserve(Class, ObjId);
        {link_to, Pid2, Tag, Type} ->
            remove_item_pid({link_from, OldPid, Tag, Type}, Pid2),
            insert_item_pid({link_to, Pid2, Tag, Type}, NewPid),
            insert_item_pid({link_from, NewPid, Tag, Type}, Pid2);
        {link_from, Pid2, Tag, Type} ->
            remove_item_pid({link_to, OldPid, Tag, Type}, Pid2),
            insert_item_pid({link_from, Pid2, Tag, Type}, NewPid),
            insert_item_pid({link_to, NewPid, Tag, Type}, Pid2)
    end,
    update_items_pid(Rest, OldPid, NewPid, State).


%% @private
send_msg(Pid, Msg, State) ->
	?DEBUG("sending msg to ~p: ~p", [Pid, Msg], State),
	Pid ! {nkdist, Msg}.



%% ===================================================================
%% Low level ETS operations
%% ===================================================================


%% @private
ets_lookup_reg(Class, ObjId) ->
    case ets:lookup(?MODULE, {reg, Class, ObjId}) of
        [] -> not_found;
        [{_, Meta, Pid}] -> {Meta, Pid}
    end.


%% @private
ets_store_reg(Class, ObjId, Meta, Pid) ->
    ets:insert(?MODULE, {{reg, Class, ObjId}, Meta, Pid}).


%% @private
ets_delete_reg(Class, ObjId) ->
    ets:delete(?MODULE, {reg, Class, ObjId}).


%% @private
ets_lookup_pid(Pid) ->
    case ets:lookup(?MODULE, {pid, Pid}) of
        [] -> not_found;
        [{_, Ref, Items}] -> {Ref, Items}
    end.

%% @private
ets_store_pid(Pid, Ref, Items) ->
    ets:insert(?MODULE, {{pid, Pid}, Ref, Items}).


%% @private
ets_delete_pid(Pid, Ref) ->
    demonitor(Ref, [flush]),
    ets:delete(?MODULE, {pid, Pid}).


%% @private
ets_lookup_reserve(Class, ObjId) ->
    case ets:lookup(?MODULE, {reserve, Class, ObjId}) of
        [] -> not_found;
        [{_, Pid}] -> Pid
    end.


%% @private
ets_store_reserve(Class, ObjId, Pid) ->
    ets:insert(?MODULE, {{reserve, Class, ObjId}, Pid}).


%% @private
ets_delete_reserve(Class, ObjId) ->
    ets:delete(?MODULE, {reserve, Class, ObjId}).


%% ===================================================================
%% Internal
%% ===================================================================

test() ->
    test1(),
    test2(),
    test3(),
    test4().


test1() ->
    Pid1 = spawn_link(fun() -> ok = register(proc, test1, obj1), timer:sleep(200) end),
    ok = register(reg, test1, obj1_b, #{pid=>Pid1, meta=>meta1}),
    timer:sleep(50),
    _ = spawn_link(fun() -> {error, {pid_conflict, Pid1}} = register(proc, test1, obj1) end),
    _ = spawn_link(fun() -> {error, {pid_conflict, Pid1}} = register(reg, test1, obj1_b) end),
    {ok, undefined, Pid1} = find(test1, obj1),
    {ok, meta1, Pid1} = find(test1, obj1_b),
    ok = register(proc, test1, obj1, #{meta=>meta2, pid=>Pid1}),
    timer:sleep(10),
    {meta2, Pid1} = ets_lookup_reg(test1, obj1),
    {meta1, Pid1} = ets_lookup_reg(test1, obj1_b),
    not_found = ets_lookup_reg(test2, obj1),
    not_found = ets_lookup_reg(test1, obj1_c),
    timer:sleep(200),
    {error, object_not_found} = find(test1, obj1),
    {error, object_not_found} = find(test1, obj1_b),
    ok.


test2() ->
	Pid1 = spawn_link(fun() -> ok = register(proc, test1, obj1), timer:sleep(200) end),
	timer:sleep(50),
    % We change de pid of the registered process
    % The vnode will call update_pid
	Pid2 = spawn_link(fun() -> ok = register(proc, test1, obj1, #{replace_pid=>Pid1}), timer:sleep(200) end),
	timer:sleep(50),
	{undefined, Pid2} = ets_lookup_reg(test1, obj1),
    not_found = ets_lookup_pid(Pid1),
    {_Ref1, [{reg, test1, obj1}]} = ets_lookup_pid(Pid2),
    timer:sleep(110),
    {_Ref1, [{reg, test1, obj1}]} = ets_lookup_pid(Pid2),
    timer:sleep(100),
    not_found = ets_lookup_pid(Pid2),
    ok.


test3() ->
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Self = self(),

    Pid1 = spawn_link(fun() -> ok = register(proc, test1, obj1), forward_msg(Ref1, Self) end),
    Pid2 = spawn_link(fun() -> ok = register(proc, test1, obj2), forward_msg(Ref2, Self) end),
    {vnode_pid, _} = wait_msg(Ref1),
    {vnode_pid, _} = wait_msg(Ref2),
    ok = link_pid(Pid1, Pid2, tag1),
    % Wait for the link to reach the other side
    {received_link, tag1} = wait_msg(Ref2),
    {_, L1} = ets_lookup_pid(Pid1),
    {_, L2} = ets_lookup_pid(Pid2),
    % In our node, we have the 'orig' entries
    % In the remote node, we have the 'dest' entries
    [{reg,test1,obj1}, {link_from, Pid2, tag1, dest}, {link_to, Pid2, tag1, orig}] = lists:sort(L1),
    [{reg,test1,obj2}, {link_from, Pid1, tag1, orig}, {link_to, Pid1, tag1, dest}] = lists:sort(L2),
    % Let's stop the process sending the link.
    % The process that received the link is notified
    Pid1 ! stop,
    stop = wait_msg(Ref1),
    {received_link_down, tag1} = wait_msg(Ref2),
    timer:sleep(50),
    not_found = ets_lookup_pid(Pid1),
    {_, [{reg,test1,obj2}]} = ets_lookup_pid(Pid2),

    % Lets register again and kill the receiver
    Pid1B = spawn_link(fun() -> ok = register(proc, test1, obj1), forward_msg(Ref1, Self) end),
    {vnode_pid, _} = wait_msg(Ref1),
    ok = link_pid(Pid1B, Pid2, tag2),
    {received_link, tag2} = wait_msg(Ref2),
    Pid2 ! stop,
    stop = wait_msg(Ref2),
    {sent_link_down, tag2} = wait_msg(Ref1),
    timer:sleep(50),
    {_, [{reg,test1,obj1}]} = ets_lookup_pid(Pid1B),
    not_found = ets_lookup_pid(Pid2),

    % Lets update the caller pid (Pid1B is still alive)
    Pid2B = spawn_link(fun() -> ok = register(proc, test1, obj2), forward_msg(Ref2, Self) end),
    {vnode_pid, _} = wait_msg(Ref2),
    ok = link_pid(Pid1B, Pid2B, tag3),
    {received_link, tag3} = wait_msg(Ref2),
    {_, L1B} = ets_lookup_pid(Pid1B),
    {_, L2B} = ets_lookup_pid(Pid2B),
    % In our node, we have the 'orig' entries
    % In the remote node, we have the 'dest' entries
    [{reg,test1,obj1}, {link_from, Pid2B, tag3, dest}, {link_to, Pid2B, tag3, orig}] = lists:sort(L1B),
    [{reg,test1,obj2}, {link_from, Pid1B, tag3, orig}, {link_to, Pid1B, tag3, dest}] = lists:sort(L2B),
    Ref1C = make_ref(),
    Pid1C = spawn_link(fun() -> ok = register(proc, test1, obj1, #{replace_pid=>Pid1B}), forward_msg(Ref1C, Self) end),
    {vnode_pid, _} = wait_msg(Ref1C),
    timer:sleep(50),
    not_found = ets_lookup_pid(Pid1B),
    {_, L1C} = ets_lookup_pid(Pid1C),
    {_, L2B2} = ets_lookup_pid(Pid2B),
    [{reg,test1,obj1}, {link_from, Pid2B, tag3, dest}, {link_to, Pid2B, tag3, orig}] = lists:sort(L1C),
    [{reg,test1,obj2}, {link_from, Pid1C, tag3, orig}, {link_to, Pid1C, tag3, dest}] = lists:sort(L2B2),
    true = is_process_alive(Pid1B),
    Pid1B ! stop,
    stop= wait_msg(Ref1),

    % Now let's update the pid of the dest
    Ref2C = make_ref(),
    Pid2C = spawn_link(fun() -> ok = register(proc, test1, obj2, #{replace_pid=>Pid2B}), forward_msg(Ref2C, Self) end),
    {vnode_pid, _} = wait_msg(Ref2C),
    timer:sleep(50),
    not_found = ets_lookup_pid(Pid2B),
    {_, L1C2} = ets_lookup_pid(Pid1C),
    {_, L2C} = ets_lookup_pid(Pid2C),
    [{reg,test1,obj1}, {link_from, Pid2C, tag3, dest}, {link_to, Pid2C, tag3, orig}] = lists:sort(L1C2),
    [{reg,test1,obj2}, {link_from, Pid1C, tag3, orig}, {link_to, Pid1C, tag3, dest}] = lists:sort(L2C),
    true = is_process_alive(Pid2B),
    Pid2B ! stop,
    stop = wait_msg(Ref2),

    % Unlink
    unlink_pid(Pid1C, Pid2C, tag3),
    timer:sleep(50),
    {_, [{reg, test1, obj1}]} = ets_lookup_pid(Pid1C),
    {_, [{reg, test1, obj2}]} = ets_lookup_pid(Pid2C),
    none = wait_msg(Ref1),
    none = wait_msg(Ref2),
    none = wait_msg(Ref1C),
    none = wait_msg(Ref2C),
    ok.

test4() ->
    Pid = spawn_link(fun() -> ok = reserve(test1, obj1), timer:sleep(100) end),
    timer:sleep(10),
    {error, {already_reserved, Pid}} = reserve(test1, obj1),
    ok = reserve(test1, obj2),
    Self = self(),
    {error, {already_reserved, Self}} = reserve(test1, obj2),
    ok = unreserve(test1, obj2),
    timer:sleep(10),
    ok = reserve(test1, obj2),
    ok = unreserve(test1, obj2),
    timer:sleep(110),
    ok = reserve(test1, obj1),
    ok = unreserve(test1, obj1).






forward_msg(Ref, Pid) ->
    receive
        stop ->
            Pid ! {Ref, stop};
        {nkdist, Msg} ->
            Pid ! {Ref, {nkdist, Msg}},
            forward_msg(Ref, Pid)
    after 500 ->
        ok
    end.


wait_msg(Ref) ->
    receive
        {Ref, {nkdist, Msg}} -> Msg;
        {Ref, stop} -> stop
    after 100 ->
        none
    end.


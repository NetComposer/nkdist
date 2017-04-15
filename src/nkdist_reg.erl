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


-export([find/2, find/3, reg/3, reg/4, link/3, link/4]).
-export([update_pid/2]).
-export([start_link/0, init/1, terminate/2, code_change/3, handle_call/3,
	handle_cast/2, handle_info/2]).
-export([test/0]).
-export_type([msg/0]).

-define(LLOG(Type, Txt, Args), lager:Type("NkDIST REG "++Txt, Args)).


%% ===================================================================
%% Public functions
%% ===================================================================

-type tag() :: term().

-type msg() ::
    {new_link, tag()} |         % A new link has been received
    {link_down, tag()} |        % A received link has fallen
    {updated_reg_pid, pid()}.   % A new process has been registered with the same name
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
-spec reg(nkdist:reg_type(), nkdist:obj_class(), nkdist:obj_id()) ->
	ok | {error, term()}.

reg(Type, Class, ObjId) ->
	reg(Type, Class, ObjId, #{}).


%% @doc Stores a new registration and updates cache
-spec reg(nkdist:reg_type(), nkdist:obj_class(), nkdist:obj_id(), nkdist:reg_opts()) ->
	ok | {error, {pid_conflict, pid()}|term()}.

reg(Type, Class, ObjId, Opts) ->
	case nkdist:register(Type, Class, ObjId, Opts) of
		ok ->
			Meta = maps:get(meta, Opts, undefined),
            Pid = maps:get(pid, Opts, self()),
			gen_server:cast(?MODULE, {put, Class, ObjId, Meta, Pid});
		{error, Error} ->
			{error, Error}
	end.


%% @doc Links this object to another object
%% DestId will be resolved, and the link will be stored at the remote node,
%% associated to both pids (destination and caller)
%% DestId will receive a {new_link, tag()} message, so that it can store the link locally
%% If the caller dies (or the full node), DestId will receive {link_down, tag()}
%% and should remove any stored info
%% If DestId dies, the link is removed at the remote node
-spec link(nkdist:obj_class(), nkdist:obj_id(), tag()) ->
	ok | {error, term()}.

link(Class, DestId, Tag) ->
	case find(Class, DestId) of
		{ok, _, DestPid} ->
            do_send_link(Class, DestId, Tag, self(), DestPid);
		{error, Error} ->
			{error, Error}
	end.


%% @doc Receives a link at current process from another object
%% We link FromObjId to us (DestId)
%% We resolve FromObjId, and store a link to us at this node
%% (DestId is only used to index the link)
%% Caller receives {new_link, tag()}
%% If FromObjId dies, caller receives {link_down, tag()}
-spec link(nkdist:obj_class(), nkdist:obj_id(), nkdist:obj_id(), tag()) ->
    ok | {error, term()}.

link(Class, OrigId, DestId, Tag) ->
    case find(Class, OrigId) of
        {ok, _, OrigPid} ->
            case find(Class, DestId) of
                {ok, _, DestPid} ->
                    do_send_link(Class, DestId, Tag, OrigPid, DestPid);
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @private Received from nkdist_vnode when a process is registered with an update pid
update_pid(OldPid, NewPid) ->
    gen_server:abcast(?MODULE, {update_pid, OldPid, NewPid}).


%% ===================================================================
%% gen_server
%% ===================================================================

%% ETS:
%% - {{reg, nkdist:obj_class(), nkdist:obj_id()}, Meta::term(), pid()}
%% - {{link, nkdist:obj_class(), Dest::nkdist:obj_id(), tag()}, Orig::pid(), Dest::pid()}
%% - {{pid, pid()}, Mon::reference(), [
%%      {reg, nkdist:obj_class(), nkdist:obj_id()} |
%%      {link_orig, Class, DestId, Tag} |
%%      {link_dest, Class, DestId, Tag}]}


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
	ets:new(?MODULE, [protected, named_table, {read_concurrency, true}]),
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

handle_cast({put, Class, ObjId, Meta, Pid}, State) ->
	insert_reg(Class, ObjId, Meta, Pid),
	{noreply, State};

handle_cast({link, Class, DestId, Tag, OrigPid, DestPid}, State) ->
	insert_link(Class, DestId, Tag, OrigPid, DestPid),
	{noreply, State};

handle_cast({update_pid, OldPid, NewPid}, State) ->
    case ets_lookup_pid(OldPid) of
        not_found ->
            ok;
        {_Ref, Items} ->
            update_items_pid(Items, OldPid, NewPid)
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
			?LLOG(notice, "received unexpected DOWN: ~p", [Pid]);
		{Ref, Items} ->
            remove_items(Items, Pid)
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
insert_reg(Class, ObjId, Meta, Pid) ->
	case ets_lookup_reg(Class, ObjId) of
		not_found ->
			ets_store_reg(Class, ObjId, Meta, Pid);
		{Meta, Pid} ->
			ok;
		{_OldMeta, Pid} ->
			ets_store_reg(Class, ObjId, Meta, Pid);
		{_OldMeta, OldPid} ->
			send_msg(OldPid, {updated_reg_pid, Pid}),
			ets_store_reg(Class, ObjId, Meta, Pid)
	end,
	insert_item_pid({reg, Class, ObjId}, Pid).


%% @private
remove_reg(Class, ObjId, Pid) ->
    ets_delete_reg(Class, ObjId),
    remove_item_pid({reg, Class, ObjId}, Pid).


%% @private
insert_link(Class, DestId, Tag, OrigPid, DestPid) ->
    ets_store_link(Class, DestId, Tag, OrigPid, DestPid),
    insert_item_pid({link_orig, Class, DestId, Tag}, OrigPid),
    insert_item_pid({link_dest, Class, DestId, Tag}, DestPid),
    send_msg(DestPid, {new_link, Tag}).


%% @private
remove_link(Class, DestId, Tag, SendMsg) ->
    case ets_lookup_link(Class, DestId, Tag) of
        {OrigPid, DestPid} ->
            ets_delete_link(Class, DestId, Tag),
            remove_item_pid({link_orig, Class, DestId, Tag}, OrigPid),
            remove_item_pid({link_dest, Class, DestId, Tag}, DestPid),
            case SendMsg of
                true ->
                    send_msg(DestPid, {link_down, Tag});
                false ->
                    ok
            end;
        not_found ->
            ok
    end.


%% @private
insert_item_pid(Item, Pid) ->
	case ets_lookup_pid(Pid) of
		not_found ->
			Ref = monitor(process, Pid),
			ets_store_pid(Pid, Ref, [Item]);
		{Ref, Items} ->
			case lists:member(Item, Items) of
				true ->
					ok;
				false ->
					ets_store_pid(Pid, Ref, [Item|Items])
			end
	end.


%% @private
remove_item_pid(Item, Pid) ->
    case ets_lookup_pid(Pid) of
        {Ref, [Item]} ->
            demonitor(Ref, [flush]),
            ets_delete_pid(Pid);
        {Ref, Items} ->
            ets_store_pid(Pid, Ref, Items--[Item]);
        not_found ->
            ok
    end.


%% @private
remove_items([], _Pid) ->
	ok;

remove_items([Item|Rest], Pid) ->
    case Item of
        {reg, Class, ObjId} ->
        	remove_reg(Class, ObjId, Pid);
        {link_orig, Class, DestId, Tag} ->
            remove_link(Class, DestId, Tag, true);
        {link_dest, Class, DestId, Tag} ->
            remove_link(Class, DestId, Tag, false)
    end,
	remove_items(Rest, Pid).


%% @private
update_items_pid([], _OldPid, _NewPid) ->
    ok;

update_items_pid([Item|Rest], OldPid, NewPid) ->
    case Item of
        {reg, Class, ObjId} ->
            remove_reg(Class, ObjId, OldPid);
        {link_orig, Class, DestId, Tag} ->
            % If the orig of the link has moved, update that side of the link
            {OldPid, DestPid} = ets_lookup_link(Class, DestId, Tag),
            remove_item_pid({link_orig, Class, DestId, Tag}, OldPid),
            ets_store_link(Class, DestId, Tag, NewPid, DestPid),
            insert_item_pid({link_orig, Class, DestId, Tag}, NewPid);
        {link_dest, Class, DestId, Tag} ->
            % If the dest of the link has moved, remove the link and create a new one
            {OrigPid, OldPid} = ets_lookup_link(Class, DestId, Tag),
            remove_link(Class, DestId, Tag, false),
            do_send_link(Class, DestId, Tag, OrigPid, NewPid)
    end,
    update_items_pid(Rest, OldPid, NewPid).


%% @private
send_msg(Pid, Msg) ->
	?LLOG(notice, "sending msg to ~p: ~p", [Pid, Msg]),
	Pid ! {?MODULE, Msg}.


%% @private
do_send_link(Class, DestId, Tag, OrigPid, DestPid) ->
    Msg = {link, Class, DestId, Tag, OrigPid, DestPid},
    case node(DestPid) of
        Node when Node == node() ->
            gen_server:cast(?MODULE, Msg);
        Node ->
            gen_server:abcast([Node], ?MODULE, Msg)
    end,
    ok.


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
ets_lookup_link(Class, DestId, Tag) ->
    case ets:lookup(?MODULE, {link, Class, DestId, Tag}) of
        [] -> not_found;
        [{_, OrigPid, DestPid}] -> {OrigPid, DestPid}
    end.


%% @private
ets_store_link(Class, DestId, Tag, OrigPid, DestPid) ->
    ets:insert(?MODULE, {{link, Class, DestId, Tag}, OrigPid, DestPid}).


%% @private
ets_delete_link(Class, DestId, Tag) ->
    ets:delete(?MODULE, {link, Class, DestId, Tag}).


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
ets_delete_pid(Pid) ->
    ets:delete(?MODULE, {pid, Pid}).


%% ===================================================================
%% Internal
%% ===================================================================

test() ->
    test1(),
    test2(),
    test3().


test1() ->
    Pid1 = spawn_link(fun() -> ok = reg(proc, test1, obj1), timer:sleep(200) end),
    ok = reg(reg, test1, obj1_b, #{pid=>Pid1, meta=>meta1}),
    timer:sleep(50),
    _ = spawn_link(fun() -> {error, {pid_conflict, Pid1}} = reg(proc, test1, obj1) end),
    _ = spawn_link(fun() -> {error, {pid_conflict, Pid1}} = reg(reg, test1, obj1_b) end),
    {ok, undefined, Pid1} = find(test1, obj1),
    {ok, meta1, Pid1} = find(test1, obj1_b),
    ok = reg(proc, test1, obj1, #{meta=>meta2, pid=>Pid1}),
    timer:sleep(10),
    {meta2, Pid1} = ets_lookup_reg(test1, obj1),
    {meta1, Pid1} = ets_lookup_reg(test1, obj1_b),
    not_found = ets_lookup_reg(test2, obj1),
    not_found = ets_lookup_reg(test1, obj1_c),
    timer:sleep(200),
    {error, obj_not_found} = find(test1, obj1),
    {error, obj_not_found} = find(test1, obj1_b),
    ok.


test2() ->
	Pid1 = spawn_link(fun() -> ok = reg(proc, test1, obj1), timer:sleep(200) end),
	timer:sleep(50),
    % We change de pid of the registered process
    % The vnode will call update_pid
	Pid2 = spawn_link(fun() -> ok = reg(proc, test1, obj1, #{replace_pid=>Pid1}), timer:sleep(200) end),
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
    Ref = make_ref(),
    Self = self(),
    Pid1 = spawn_link(fun() -> ok = reg(proc, test1, obj1), timer:sleep(200) end),
    Pid2 = spawn_link(fun() -> ok = reg(proc, test1, obj2), forward_msg(Ref, Self) end),
    timer:sleep(50),
    ok = link(test1, obj1, obj2, tag1),
    timer:sleep(50),
    {Pid1, Pid2} = ets_lookup_link(test1, obj2, tag1),
    {nkdist, {vnode_pid, _}} = wait_msg(Ref),
    {nkdist_reg, {new_link, tag1}} = wait_msg(Ref),
    timer:sleep(110),
    {nkdist_reg, {link_down, tag1}} = wait_msg(Ref),
    not_found = ets_lookup_link(test1, obj2, tag1),
    not_found = ets_lookup_pid(Pid1),
    {_, [{reg, test1, obj2}]} = ets_lookup_pid(Pid2),


    % Lets register again, and 'move' the origin
    Pid3A = spawn_link(fun() -> ok = reg(proc, test1, obj3), timer:sleep(200) end),
    timer:sleep(50),
    ok = link(test1, obj3, obj2, tag2),
    Pid3B = spawn_link(fun() -> ok = reg(proc, test1, obj3, #{replace_pid=>Pid3A}), timer:sleep(200) end),
    timer:sleep(50),
    {Pid3B, Pid2} = ets_lookup_link(test1, obj2, tag2),
    not_found = ets_lookup_pid(Pid3A),
    {_, L1} = ets_lookup_pid(Pid3B),
    [{reg, test1, obj3}, {link_orig, test1, obj2, tag2}] = lists:sort(L1),
    {_, L2} = ets_lookup_pid(Pid2),
    [{reg, test1, obj2}, {link_dest, test1, obj2, tag2}] = lists:sort(L2),
    {Pid3B, Pid2} = ets_lookup_link(test1, obj2, tag2),
    timer:sleep(250),
    {nkdist_reg, {new_link, tag2}} = wait_msg(Ref),
    {nkdist_reg, {link_down, tag2}} = wait_msg(Ref),
    not_found = ets_lookup_pid(Pid3B),
    {_, [{reg, test1, obj2}]} = ets_lookup_pid(Pid2),
    not_found = ets_lookup_link(test1, obj2, tag2),

    % Lets register again, and 'move' the destination
    Pid4 = spawn_link(fun() -> ok = reg(proc, test1, obj4), timer:sleep(200) end),
    timer:sleep(50),
    ok = link(test1, obj4, obj2, tag3),
    {nkdist_reg, {new_link, tag3}} = wait_msg(Ref),
    Pid2B = spawn_link(fun() -> ok = reg(proc, test1, obj2, #{replace_pid=>Pid2}), forward_msg(Ref, Self) end),
    timer:sleep(50),
    {Pid4, Pid2B} = ets_lookup_link(test1, obj2, tag3),
    not_found = ets_lookup_pid(Pid2),
    {_, L3} = ets_lookup_pid(Pid2B),
    [{reg,test1,obj2}, {link_dest,test1,obj2,tag3}] = lists:sort(L3),
    {_, L4} = ets_lookup_pid(Pid4),
    [{reg,test1,obj4}, {link_orig,test1,obj2,tag3}] = lists:sort(L4),
    {nkdist, {vnode_pid, _}} = wait_msg(Ref),
    {nkdist_reg, {new_link, tag3}} = wait_msg(Ref),
    {nkdist_reg, {link_down, tag3}} = wait_msg(Ref),

    not_found = ets_lookup_link(test1, obj2, tag3),
    {_, [{reg,test1,obj2}]} = ets_lookup_pid(Pid2B),
    not_found = ets_lookup_pid(Pid4),
    Pid2B ! stop,
    stop = wait_msg(Ref),
    ok.










forward_msg(Ref, Pid) ->
    receive
        stop ->
            Pid ! {Ref, stop};
        Msg ->
            Pid ! {Ref, Msg},
            forward_msg(Ref, Pid)
    after 500 ->
        ok
    end.


wait_msg(Ref) ->
    receive {Ref, Msg} ->
        Msg
    after 500 ->
        error(?LINE)
    end.

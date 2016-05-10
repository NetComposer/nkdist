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

%% @doc Temporary testing
-module(nkdist_test).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([s/0]).
-export([single/0, multi/0, search/0]).
-export([start/0, start/3]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).


s() ->
    catch sys:terminate(rebar_agent, normal),
    nklib_reloader:start(),
    nkdist_admin:quick_join("dev1@192.168.0.9").



single() ->
    Self = self(),
    ok = nkdist:register(reg, c1, a),
    {ok, reg, [{undefined, Self}]} = nkdist:find(c1, a),
    {error, {already_used, reg}} = nkdist:register(mreg, c1, a),
    ok = nkdist:register(mreg, c2, a),
    ok = nkdist:unregister(c2, a),
    {error, obj_not_found} = nkdist:find(c2, a),

    ok = nkdist:register(reg, c1, a, #{meta=>1}),
    {ok, reg, [{1, Self}]} = nkdist:find(c1, a),
    ok = nkdist:unregister(c1, a),
    {error, obj_not_found} = nkdist:find(c1, a),

    Pid1 = spawn(
        fun() ->
            ok = nkdist:register(reg, c1, a, #{meta=>a1}),
            timer:sleep(200)
        end),
    timer:sleep(50),
    {ok, reg, [{a1, Pid1}]} = nkdist:find(c1, a),
    {error, {already_registered, Pid1}} = nkdist:register(reg, c1, a),
    timer:sleep(300),
    {error, obj_not_found} = nkdist:find(c1, a),
    ok.


multi() ->
    Self = self(),
    ok = nkdist:register(mreg, c1, a),
    {ok, mreg, [{undefined, Self}]} = nkdist:find(c1, a),
    {error, {already_used, mreg}} = nkdist:register(reg, c1, a),
    ok = nkdist:register(mreg, c1, a, #{meta=>1}),
    {ok, mreg, [{1, Self}]} = nkdist:find(c1, a),
    ok = nkdist:unregister(c1, a),
    {error, obj_not_found} = nkdist:find(c1, a),

    ok = nkdist:register(mreg, c1, a),
    Pid1 = spawn(
        fun() ->
            ok = nkdist:register(mreg, c1, a, #{meta=>a1}),
            ok = nkdist:register(mreg, c1, a, #{meta=>a2}),
            timer:sleep(100),
            ok = nkdist:unregister(c1, a),
            timer:sleep(500)
        end),
    Pid2 = spawn(
        fun() ->
            ok = nkdist:register(mreg, c1, a, #{meta=>a3}),
            timer:sleep(200)
        end),
    timer:sleep(50),
    {ok, mreg, List1} = nkdist:find(c1, a),
    [{a2, Pid1}, {a3, Pid2}, {undefined, Self}] = lists:sort(List1),
    timer:sleep(100),
    {ok, mreg, List2} = nkdist:find(c1, a),
    [{a3, Pid2}, {undefined, Self}] = lists:sort(List2),
    timer:sleep(150),
    {ok, mreg, [{undefined, Self}]} = nkdist:find(c1, a),
    ok = nkdist:unregister(c1, a),
    {error, obj_not_found} = nkdist:find(c1, a),
    ok.


search() ->
    nkdist:register(reg, s1, a),
    nkdist:register(mreg, s1, b),
    nkdist:register(reg, s1, c),
    nkdist:register(proc, s2, d),
    nkdist:register(reg, s3, e),
    nkdist:register(master, s3, f),
    {ok, []} = nkdist:search_class(s0),
    {ok, S1} = nkdist:search_class(s1),
    [a, b, c] = lists:sort(S1),
    {ok, [d]} = nkdist:search_class(s2),
    {ok, S3} = nkdist:search_class(s3),
    [e, f] = lists:sort(S3),
    {ok, []} = nkdist:search_class(s4),
    nkdist:unregister(s1, a),
    nkdist:unregister(s1, b),
    nkdist:unregister(s1, c),
    nkdist:unregister(s2, d),
    nkdist:unregister(s3, e),
    nkdist:unregister(s3, f),
    {ok, []} = nkdist:search_class(s1),
    {ok, []} = nkdist:search_class(s2),
    {ok, []} = nkdist:search_class(s3),
    ok.



%% @private
start() ->
    start(reg, c, id).



%% @doc
start(Type, Class, Id) ->
    gen_server:start(?MODULE, [Type, Class, Id], []).


-define(LOG(Level, Txt, Args, State),
    lager:Level("NK Test ~p ~p "++Txt, [State#state.type, self()|Args])).



% ===================================================================
%% gen_server behaviour
%% ===================================================================

-record(state, {
    type :: atom(),
    class :: term(),
    id :: term(),
    vnode_pid :: pid(),
    vnode_mon :: reference(),
    master :: pid(),
    leader :: pid(),
    must_move :: node()
}).


%% @private
init([Type, Class, Id]) ->
    {ok, Node, Idx} = nkdist:get_vnode(Class, Id),
    Pos = nkdist_util:idx2pos(Idx),
    lager:info("Starting ~p proccess {~p, ~p} at ~p (~p, ~p)", 
               [Type, Class, Id, Node, Pos, Idx]),
    State = #state{type=Type, class=Class, id=Id},
    case do_register(State) of
        ok ->
            {ok, State};
        {error, Error} ->
            lager:error("Error registering ~p process: ~p", [Type, Error]),
            {stop, normal}
    end.


%% @private
handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_cast(Msg, State) -> 
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_info({nkdist, NkDist}, State) ->
    handle_nkdist(NkDist, State);

handle_info(stop, State) ->
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, #state{vnode_pid=Pid}=State) ->
    ?LOG(notice, "Vnode has failed!", [], State),
    case do_register(State) of
        ok ->
            ?LOG(info, "re-registered ok", [], State),
            {noreply, State};
        {error, Error} ->
            ?LOG(info, "could not re-register: ~p", [Error], State),
            {noreply, State}
    end;

handle_info(Info, State) -> 
    lager:warning("Module ~p received unexpected info: ~p (~p)", [?MODULE, Info, State]),
    {noreply, State}.


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
terminate(Reason, State) ->
    ?LOG(info, "stopped: ~p", [Reason], State).
    


% ===================================================================
%% Internal
%% ===================================================================

do_register(#state{type=Type, class=Class, id=Id}) ->
    catch nkdist:register(Type, Class, Id).


handle_nkdist({vnode_pid, Pid}, #state{vnode_pid=OldPid, vnode_mon=Mon}=State) ->
    case Pid of
        OldPid ->
            {noreply, State};
        _ ->
            case OldPid of
                undefined ->
                    ?LOG(info, "vnode is ~p", [Pid], State);
                _ ->
                    nklib_util:demonitor(Mon),
                    ?LOG(notice, "vnode has changed! (~p)", [Pid], State)
            end,
            {noreply, State#state{vnode_pid=Pid, vnode_mon=monitor(process, Pid)}}
    end;

handle_nkdist(NkDist, State) ->
    ?LOG(info, "nkdist msg at ~p: ~p", [self(), NkDist], State),
    {noreply, State}.








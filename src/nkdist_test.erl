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
-export([d1/0, my_reg_loop/1]).


s() ->
    sys:terminate(rebar_agent, normal),
    nklib_reloader:start(),
    nkdist_admin:quick_join("dev1@127.0.0.1").



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

   
d1() ->
    spawn_link(fun() -> my_reg() end).




my_reg() ->
    ok = nkdist:register(proc, c1, p4),
    my_reg_loop(#{}).


my_reg_loop(State) ->
    receive
        stop ->
            ok;
        Any ->
            lager:warning("RECV: ~p", [Any]),
            nkdist_test:my_reg_loop(State)
    end.














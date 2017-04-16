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

%% @doc NkDIST LIB OTP Application Module
-module(nkdist_app).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(application).

-export([start/0, start/1, start/2, stop/1]).
-export([get/1, put/2]).

-include("nkdist.hrl").

-compile({no_auto_import, [get/1, put/2]}).

-define(APP, nkdist).


%% ===================================================================
%% Private
%% ===================================================================

%% @doc Starts stand alone.
-spec start() -> 
    ok | {error, Reason::term()}.

start() ->
    start(temporary).


%% @doc Starts stand alone.
-spec start(permanent | transient | temporary) -> 
    ok | {error, Reason::term()}.

start(Type) ->
    nkdist_util:ensure_dir(),
    case nklib_util:ensure_all_started(?APP, Type) of
        {ok, _Started} ->
            riak_core:wait_for_service(nkdist),
            ok;
        Error ->
            Error
    end.


%% @private OTP standard start callback
start(_Type, _Args) ->
    {ok, Vsn} = application:get_key(?APP, vsn),
    nkdist_util:store_idx_cache(),
    lager:info("NkDIST v~s is starting", [Vsn]),
    {ok, Pid} = nkdist_sup:start_link(),
    Syntax = #{
        vnode_workers => integer,
        debug => boolean,
        '__defaults' => #{
            debug => false,
            vnode_workers => 5
        }
    },
    case nklib_config:load_env(?APP, Syntax) of
        {ok, _} ->
            wait_for_riak(),
            {ok, Pid};
        {error, Error} ->
            lager:error("Error parsing config: ~p", [Error]),
            error(Error)
    end.


%% @private OTP standard stop callback
stop(_) ->
    ok.


%% @doc gets a configuration value
get(Key) ->
    get(Key, undefined).


%% @doc gets a configuration value
get(Key, Default) ->
    nklib_config:get(?APP, Key, Default).


%% @doc updates a configuration value
put(Key, Value) ->
    nklib_config:put(?APP, Key, Value).


%% @private
wait_for_riak() ->
    riak_core:register(?APP, [{vnode_module, nkdist_vnode}]),
    ok = riak_core_ring_events:add_guarded_handler(
           nkdist_ring_event_handler, []),
    ok = riak_core_node_watcher_events:add_guarded_handler(
           nkdist_node_event_handler, []),
    wait_for_metadata(),
    %% Force the creation of vnodes before waiting for 
    %% 'vnode_management_timer' time
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring_handler:ensure_vnodes_started(Ring).


%% @private
wait_for_metadata() ->
    case whereis(riak_core_metadata_manager) of
        Pid when is_pid(Pid) ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_metadata()
    end.



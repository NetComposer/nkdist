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

%% @doc Master Management
%% This behaviour is nearly equivalent to a standar gen_server that
%% registers locally under the 'Callback' atom, and it is intended to
%% be started at several nodes at the cluster.
%% One of them will be elected master, and the callback handle_master/2 
%% will be called with its pid(), or 'undefined' if the vnode fails.
-module(nkdist_gen_server).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([start_link/3, start/3, get_master/1, get_master/2, call/2, call/3, cast/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).


%% ===================================================================
%% Behaviour
%% ===================================================================

-callback init(Args :: term()) ->
    {ok, State :: term()} | {ok, State :: term(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.

-callback handle_call(Request :: term(), From :: {pid(), Tag :: term()},
                      State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {reply, Reply :: term(), NewState :: term(), timeout() | hibernate} |
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.

-callback handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.

-callback handle_info(Info :: timeout | term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.

-callback terminate(Reason :: (normal | shutdown | {shutdown, term()} |
                               term()),
                    State :: term()) ->
    term().

-callback code_change(OldVsn :: (term() | {down, term()}), State :: term(),
                      Extra :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

-callback handle_master(pid()|undefined, State :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term(), NewState :: term()}.



%% ===================================================================
%% Public
%% ===================================================================


%% @doc Starts the process
-spec start_link(atom(), term(), list()) ->
    {ok, pid()} | {error, term}.

start_link(Callback, Arg, Opts) ->
    gen_server:start_link({local, Callback}, ?MODULE, {Callback, Arg}, Opts).


%% @doc Starts the process
-spec start(atom(), term(), list()) ->
    {ok, pid()} | {error, term}.

start(Callback, Arg, Opts) ->
    gen_server:start({local, Callback}, ?MODULE, {Callback, Arg}, Opts).


%% @doc Equivalent to get_master(Callback, 5000).
-spec get_master(atom()) ->
    {ok, pid()|undefined}.

get_master(Callback) ->
    get_master(Callback, 5000).


%% @doc Gets the current master
-spec get_master(atom(), pos_integer()) ->
    {ok, pid()|undefined}.

get_master(Callback, Timeout) ->
    gen_server:call(Callback, nkdist_get_master, Timeout).


%% @doc Equivalent to call(Callback, Msg, 5000)
-spec call(atom(), term()) ->
    term() | {error, no_master}.

call(Callback, Msg) ->
    call(Callback, Msg, 5000).


%% @doc Cals to the current master
-spec call(atom(), term(), pos_integer()|infinity) ->
    term() | {error, no_master}.

call(Callback, Msg, Timeout) ->
    case get_master(Callback, Timeout) of
        {ok, Pid} when is_pid(Pid) ->
            gen_server:call(Pid, Msg, Timeout);
        {ok, undefined} ->
            {error, no_master}
    end.


%% @doc Cals to the current master
-spec cast(atom(), term()) ->
    ok | {error, no_master}.

cast(Callback, Msg) ->
    case get_master(Callback, 60000) of
        {ok, Pid} when is_pid(Pid) ->
            gen_server:cast(Pid, Msg);
        {ok, undefined} ->
            {error, no_master}
    end.



% ===================================================================
%% gen_server
%% ===================================================================



-record(state, {
    callback :: atom(),
    vnode :: pid(),
    master :: pid(),
    user :: term()
}).

-define(POSMOD, #state.callback).
-define(POSUSER, #state.user).


%% @private 
init({Callback, Arg}) ->
    State = register(#state{callback=Callback}),
    nklib_gen_server:init(Arg, State, ?POSMOD, ?POSUSER).


%% @private
handle_call(nkdist_get_master, _From, #state{master=Master}=State) ->
    {reply, {ok, Master}, State};

handle_call(Msg, From, State) -> 
    nklib_gen_server:handle_call(Msg, From, State, ?POSMOD, ?POSUSER).


%% @private
handle_cast(Msg, State) -> 
    nklib_gen_server:handle_cast(Msg, State, ?POSMOD, ?POSUSER).


%% @private
handle_info(nkdist_register, State) ->
    {noreply, register(State)};

handle_info({nkdist_master, _Callback, Master}, #state{master=Master}=State) ->
    {noreply, State};

handle_info({nkdist_master, _Callback, Master}, State) ->
    State1 = State#state{master=Master},
    case
        nklib_gen_server:handle_any(handle_master, [Master], State1, ?POSMOD, ?POSUSER)
    of
        {ok, State2} -> {noreply, State2};
        {error, Error, State2} -> {stop, Error, State2}
    end;

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{vnode=Pid}=State) ->
    case Reason of
        normal -> lager:info("NkDIST master: vnode has stopped");
        _ -> lager:warning("NkDIST master: vnode has failed: ~p", [Reason])
    end,
    self() ! {nkdist_master, State#state.callback, undefined},
    {noreply, register(State)};

handle_info(Info, State) -> 
    nklib_gen_server:handle_info(Info, State, ?POSMOD, ?POSUSER).


%% @private
-spec code_change(term(), #state{}, term()) ->
    {ok, #state{}}.

code_change(OldVsn, State, Extra) ->
    nklib_gen_server:code_change(OldVsn, State, Extra, ?POSMOD, ?POSUSER).


%% @private
-spec terminate(term(), #state{}) ->
    ok.

terminate(Reason, State) ->  
    nklib_gen_server:terminate(Reason, State, ?POSMOD, ?POSUSER).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Internal %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @private
register(#state{callback=Callback}=State) ->
    case nkdist:register(Callback) of
        {ok, VNode} ->
            monitor(process, VNode),
            State#state{vnode=VNode};
        {error, Error} ->
            lager:notice("NkDIST: Callback ~p could not register: ~p", [Callback, Error]),
            erlang:send_after(500, self(), nkdist_register),
            State
    end.






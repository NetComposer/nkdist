-module(nkdist_proc_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).
-behaviour(nkdist_proc).

-export([start/2, start_and_join/2, join/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,   
         handle_cast/2, handle_info/2]).


%% ===================================================================
%% Proc Behaviour
%% ===================================================================


start(ProcId, Args) ->
    gen_server:start_link(?MODULE, {proc_id, ProcId, Args}, []).

start_and_join(ProcId, OldPid) ->
    gen_server:start_link(?MODULE, {join, ProcId, OldPid}, []).

join(Pid, OldPid) ->
    gen_server:call(Pid, {join, OldPid}).



%% ===================================================================
%% gen_server
%% ===================================================================


-record(state, {
    id,
    data
}).


%% @private 
-spec init(term()) ->
    {ok, #state{}}.

init({proc_id, ProcId, _Args}) ->
    Data = crypto:rand_uniform(0, 100),
    lager:notice("Test server ~p (~p) started with: ~p", [ProcId, self(), Data]),
    {ok, #state{id=ProcId, data=Data}};

init({join, ProcId, Pid}) ->
    case catch gen_server:call(Pid, freeze, infinity) of
        {ok, Data} ->
            lager:notice("Test server ~p (~p) started from ~p (~p)", 
                          [ProcId, self(), Pid, Data]),
            {ok, #state{id=ProcId, data=Data}};
        _ ->
            {stop, could_not_start}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}} | {noreply, #state{}}.

handle_call(freeze, _From, #state{data=Data}=State) ->
    lager:notice("Test server ~p (~p) freezed", [self(), Data]),
    {stop, normal, {ok, Data}, State};

handle_call({join, Pid}, _From, #state{id=ProcId, data=Data}=State) ->
    case catch gen_server:call(Pid, freeze, infinity) of
        {ok, NewData} ->
            lager:notice("Test server ~p (~p) joined ~p (~p, ~p)", 
                          [ProcId, self(), Pid, NewData, Data]),
            {reply, ok, State#state{data=max(NewData, Data)}};
        _ ->
            lager:notice("Test server ~p (~p) could not join ~p", 
                          [ProcId, self(), Pid]),
            {reply, {error, could_not_join}, State}
    end;

handle_call(get_data, _From, #state{data=Data}=State) ->
    {reply, {ok, Data}, State};

handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_cast(term(), #state{}) ->
    {noreply, #state{}}.

handle_cast(stop, State) ->
    {stop, normal, State};
    
handle_cast(Msg, State) ->
    lager:error("Module ~p received unexpected cast: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}}.

handle_info(Msg, State) ->
    lager:error("Module ~p received unexpected info: ~p", [?MODULE, Msg]),
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
    lager:notice("Test server stopped: ~p", [self()]),
    ok.



%% ===================================================================
%% Internal
%% ===================================================================



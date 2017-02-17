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

%% @private NkDIST LIB main supervisor
-module(nkdist_sup).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(supervisor).

-export([init/1, start_link/0]).
-export([start_coverage_fsm_sup/0]).

-include("nkdist.hrl").

%% @private
-spec start_link() ->
    {ok, pid()}.

start_link() ->
    ChildsSpec = [
        {nkdist_coverage_sup, 
            {?MODULE, start_coverage_fsm_sup, []},
            permanent,
            infinity,
            supervisor,
            [?MODULE]},
        {nkdist_vnode_master,
            {riak_core_vnode_master, start_link, [nkdist_vnode]},
            permanent, 
            5000, 
            worker, 
            [riak_core_vnode_master]}
    ],
    supervisor:start_link({local, ?MODULE}, ?MODULE, {{one_for_one, 10, 60}, 
                          ChildsSpec}).


%% @private
init(ChildSpecs) ->
    {ok, ChildSpecs}.


%% @private
start_coverage_fsm_sup() ->
    supervisor:start_link({local, nkdist_coverage_sup}, ?MODULE, 
        {{simple_one_for_one, 3, 60}, [
            {undefined,
                {riak_core_coverage_fsm, start_link, [nkdist_coverage]},
                temporary,
                5000,
                worker,
                [nkdist_coverage]
        }]}).


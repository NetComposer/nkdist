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

-module(nkdist_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([ensure_dir/0, store_idx_cache/0, idx2pos/0, idx2pos/1, pos2idx/1]).


%% ===================================================================
%% Public
%% ===================================================================

ensure_dir() ->
    application:load(riak_core),
    {ok, DataDir} = application:get_env(riak_core, platform_data_dir),
    RingFile = filename:join([DataDir, "ring", "dummy"]),
    filelib:ensure_dir(RingFile).


%% @private Stored a mapping from (long) IDX numbers to short indices
store_idx_cache() ->
	{ok, Ring} = riak_core_ring_manager:get_my_ring(),
	NumParts = riak_core_ring:num_partitions(Ring),
	OwnersData = riak_core_ring:all_owners(Ring),
	%% 0 to NumParts-1
	Idxs2Pos = lists:zip(
		lists:seq(0, NumParts-1), 
		[Idx || {Idx, _N} <- OwnersData]
	),
	riak_core_mochiglobal:put(nkdist_idx2pos, Idxs2Pos).


%% @doc Converts a (long) Idx to a short pos
idx2pos() ->
	'mochiglobal:nkdist_idx2pos':term().



idx2pos(Idx) ->
	{Pos, Idx} = lists:keyfind(Idx, 2, idx2pos()),
	Pos.
	
pos2idx(Num) ->	
	{Num, Idx} = lists:keyfind(Num, 1, idx2pos()),
	Idx.
	




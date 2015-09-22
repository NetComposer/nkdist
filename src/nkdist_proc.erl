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

%% @doc Cluster Proc Behaviour
-module(nkdist_proc).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').


%% ===================================================================
%% Types
%% ===================================================================

%% @doc Start a new process
%% This callback is called when the start of a new process has been requested
%% using nkdist:start_proc/3.
%% You must start a new Erlang process and return its pid().
%% If you link to the calling process (the vnode process), you will receive an
%% 'EXIT' when the vnode is shutted down.
-callback start(nkdist:proc_id(), Args::term()) ->
	{ok, pid()} | {error, term()}.


%% @doc Starts a new clone process
%% This callback is called when an existing process must be moved from an old
%% node to a new node. You receive the pid() of the old (still running) process,
%% so you can:
%% - get any information from it
%% - put it in any "stopping" state so that it does not accept new requests,
%%   or return the new pid()
%% - stop the process or schedule its stop once it has no more work to do
-callback start_and_join(nkdist:proc_id(), pid()) ->
	{ok, pid()} | {error, term()}.


%% @doc Joins two existing processes
%% This callback is called when an existing process must be moved from an old
%% node to a new node, but the process already exists in the new node.
%% This can happen because of a network partition, while the process was
%% stared at both sides of the network
%% You receive the pid() of the 'winning' process, and the pid() of the old
%% process, and must 'join' its states and stop the old one.
-callback join(Current::pid(), Old::pid()) ->
	ok | {error, term()}.


%% @doc Optional callback to use a custom hashing function
%% -callback get_hash(nkdist:proc_id()) ->
%% 		binary(). 





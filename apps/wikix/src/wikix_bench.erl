%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2017 Pundun Labs AB
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
%% implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------
%% @doc
%% Module Description:
%% @end
%%%===================================================================


-module(wikix_bench).

-export([test/1]).

%% Spawned functions
-export([server/3,
	 client/3,
	 fire_stop/1]).

-define(SPAWNS, 8).
-define(CHUNK, 5000).
-define(DURATION, 60000). %%1 Min

%%%===================================================================
%%% API functions
%%%===================================================================
test(Options) when is_map(Options) ->
    test_load(Options).

-spec test_load(Options :: map()) ->
    ok.
test_load(Options) ->
    Connection = maps:get(connection, Options, 'pundun97ae64@sitting'),
    DataModel = maps:get(data_model, Options, kv),
    N = maps:get(clients, Options, 8),
    Duration = maps:get(duration, Options, ?DURATION),
    io:format("~p:~p(~p).~n", [?MODULE, ?FUNCTION_NAME, Connection]),
    {ok, Session} = connect(Connection),
    rpc(Session, delete_table, ["kv"]),
    rpc(Session, delete_table, ["array"]),
    rpc(Session, delete_table, ["map"]),
    {ok, Tab} = create_table(Session, DataModel),
    ok = disconnect(Session),
    Report = #{successful_writes => 0,
	       unsuccessful_writes => 0,
	       successful_reads => 0,
	       unsuccessful_reads => 0},
    SPid = spawn(?MODULE, server, [Duration, N, Report]),
    _MonitorRef = erlang:monitor(process, SPid),
    register(my_server, SPid),
    [spawn(?MODULE, client, [SPid, Connection, Tab] ) || _ <- lists:seq(1,N)],
    {ok, SPid}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
create_table(Session, ATab) ->
    Tab = atom_to_list(ATab),
    TableOptions = [{type, rocksdb},
		    {num_of_shards, 1},
		    {distributed, false},
		    {hashing_method, uniform},
		    {data_model, ATab}],
    io:format("[~p:~p] creating with options: ~p~n",[?MODULE, ?LINE, TableOptions]),
    ok = rpc(Session, create_table, [Tab, ["title"], TableOptions]),
    {ok, Tab}.
    
connect(Node) when is_atom(Node) ->
    {ok, Node};
connect(Pid) when is_pid(Pid) ->
    {ok, Pid};
connect({Host, Port, User, Pass}) ->
    pbpc:connect(Host, Port, User, Pass).

disconnect(Node) when is_atom(Node)->
    ok;
disconnect(Session) when is_pid(Session) ->
    pbpc:disconnect(Session).

fire_stop(Pid) ->
    Pid ! stop.

server(Duration, N, Report) ->
    Start = os:timestamp(),
    timer:apply_after(Duration, ?MODULE, fire_stop, [self()]),
    io:format("[~p:~p] Starting at: ~p~n",[?MODULE, ?LINE, calendar:now_to_local_time(Start)]),
    server(run, Start, N, Report).

server(_, StartTs, 0, Report) ->
    report(StartTs, os:timestamp(), Report);
server(Run, StartTs, N, Report) ->
    receive
	stop ->
	    server(stop, StartTs, N, Report);
	{write_result, ok} ->
	    SW = maps:get(successful_writes, Report),
	    server(Run, StartTs, N, Report#{successful_writes => SW + ?CHUNK});
	{write_result, {error, _}} ->
	    UW = maps:get(unsuccessful_writes, Report),
	    server(Run, StartTs, N, Report#{unsuccessful_writes => UW + ?CHUNK});
	{read_result, ok} ->
	    SR = maps:get(successful_reads, Report),
	    server(Run, StartTs, N, Report#{successful_reads => SR + ?CHUNK});
	{read_result, {error, _}} ->
	    UR = maps:get(unsuccessful_reads, Report),
	    server(Run, StartTs, N, Report#{unsuccessful_reads => UR + ?CHUNK});
	{client, Pid, register} ->
	    erlang:monitor(process, Pid),
	    server(Run, StartTs, N, Report);
	{client, Pid, more} ->
	    Pid ! {server, Run},
	    server(Run, StartTs, N, Report);
	{'DOWN', _MonitorRef, process, _Object, _Info} ->
	    server(Run, StartTs, N-1, Report)
    end.

report(Start, Stop, #{successful_writes := SW,
		      unsuccessful_writes := UW,
		      successful_reads := SR,
		      unsuccessful_reads := UR}) ->
    Time = timer:now_diff(Stop, Start)/1000/1000,
    io:format("[~p:~p] It took ~.2f seconds.~n",[?MODULE, ?LINE, Time]),
    io:format("[~p:~p] Successful Writes: ~p Failed Writes: ~p ",[?MODULE, ?LINE, SW, UW]),
    io:format("Successful Reads: ~p Failed Reads: ~p~n",[SR, UR]),
    SWR = trunc(SW / Time),
    UWR = trunc(UW / Time),
    TWR = trunc((SW+UW) / Time),
    SRR = trunc(SR / Time),
    URR = trunc(UR / Time),
    TRR = trunc((SR+UR) / Time),
    io:format("[~p:~p] Success write rate: ~p/s Fail write rate: ~p/s ",[?MODULE, ?LINE, SWR, UWR]),
    io:format("Success read rate: ~p/s Fail read rate: ~p/s~n",[SRR, URR]),
    io:format("[~p:~p] Total write rate: ~p/s Total read rate: ~p/s~n",[?MODULE, ?LINE, TWR, TRR]),
    io:format("[~p:~p] Server stopping..~n",[?MODULE, ?LINE]).

client(SPid, Connection, Tab) ->
    SPid ! {client, self(), register},
    {ok, Session} = connect(Connection),
    client_loop(SPid, Session, Tab).

client_loop(SPid, Session, Tab) ->
    %%Start = os:timestamp(),
    SPid ! {client, self(), more},
    receive
	{server, run} ->
	    ok = load(SPid, Session, Tab),
	    %%Stop = os:timestamp(),
	    %%Time = timer:now_diff(Stop, Start)/1000/1000,
	    %io:format("It took ~.2f seconds to load ~p entries.~n", [Time, ?CHUNK]),
	    client_loop(SPid, Session, Tab);
	{server, stop} ->
	    ok = disconnect(Session)
    end.

load(SPid, Session, Tab) ->
    Data =
	case Tab of
	    "kv" ->
		[{data(integer),{data(binary),data(map),data(string)}}];
	    _ ->
		[{"field1", data(integer)},
		 {"field2", data(binary)},
		 {"field3", data(map)},
		 {"field4", data(string)}]
	end,
    Terms = [[Tab, [{"title", make_key(N)}], Data] || N <- lists:seq(1, ?CHUNK)],
    write_terms(SPid, Session, Terms),
    read_terms(SPid, Session, Terms).

write_terms(SPid, Session, Terms) ->
    Res = [rpc(Session, write, Args) || Args <- Terms],
    SPid ! {write_result, get_res(lists:usort(Res))},
    ok.

read_terms(SPid, Session, Terms) ->
    Res = [element(1,rpc(Session, read, [T, K])) || [T, K, _] <- Terms],
    SPid ! {read_result, get_res(lists:usort(Res))},
    ok.

get_res([ok]) ->
    ok;
get_res(_) ->
    {error, failed}.

rpc(Node, Fun, Args) when is_atom(Node)->
    rpc:call(Node, enterdb, Fun, Args);
rpc(Session, Fun, Args) ->
    apply(pbpc, Fun, [Session | Args]).

make_key(N) ->
    #{key=>erlang:unique_integer([positive,monotonic]) * N}.

data(map)->
    #{procedure => {read_range,#{end_key => [#{name => "map",value => #{type => {map,#{values => #{"a" => #{type => {int,1}},"b" => #{type => {int,1}},"c" => #{type => {int,1}}}}}}},#{name => "id",value => #{type => {string,"same"}}}],limit => 2,start_key => [#{name => "map",value => #{type => {map,#{values => #{"a" => #{type => {int,2}},"b" => #{type => {int,2}},"c" => #{type => {int,2}}}}}}},#{name => "id",value => #{type => {string,"same"}}}],table_name => "pundunpy_test_table"}},transaction_id => 7,version => #{major => 0,minor => 1}};
data(binary) ->
    <<10,2,16,1,16,7,34,175,1,34,172,1,10,82,10,12,10,2,105,100,18,6,50,4,115,97,109,101,10,36,10,3,109,97,112,18,29,66,27,10,7,10,1,97,18,2,16,2,10,7,10,1,98,18,2,16,2,10,7,10,1,99,18,2,16,2,18,13,10,6,110,117,109,98,101,114,18,3,50,1,50,18,13,10,4,116,101,120,116,18,5,50,3,84,119,111,10,82,10,12,10,2,105,100,18,6,50,4,115,97,109,101,10,36,10,3,109,97,112,18,29,66,27,10,7,10,1,97,18,2,16,1,10,7,10,1,98,18,2,16,1,10,7,10,1,99,18,2,16,1,18,13,10,6,110,117,109,98,101,114,18,3,50,1,49,18,13,10,4,116,101,120,116,18,5,50,3,79,110,101,18,2,8,1>>;
data(integer) ->
    99892;
data(string) ->
    "The protocol buffer compiler produces Python output when invoked with the --python_out= command-line flag. The parameter to the --python_out= option is the directory where you want the compiler to write your Python output. The compiler creates a .py file for each .proto file input. The names of the output files are computed by taking the name of the .proto file and making two changes".

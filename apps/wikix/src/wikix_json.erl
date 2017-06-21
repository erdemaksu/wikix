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


-module(wikix_json).


%% API functions
-export([import/2,
	 import/3]).

%% Spawned functions
-export([server/5,
	 client/3]).

-include_lib("gb_log/include/gb_log.hrl").

-define(CHUNK, 25).

%%%===================================================================
%%% API functions
%%%===================================================================
import(Dir, N) ->
    {ok, Session} = pbpc:connect("127.0.0.1", 8887, "admin", "admin"),
    import(Session, Dir, N),
    ok = pbpc:disconnect(Session).

-spec import(Connection :: term(),
	     Dir :: string(),
	     N :: integer() | undefined) ->
    ok.
import(Connection, Dir, N) ->
    {ok, Filenames} = file:list_dir(Dir),
    %io:format("[~p:~p] Filenames: ~p~n", [?MODULE, ?LINE, Filenames]),

    Tab = filename:basename(Dir),
    {ok, Session} = connect(Connection),
    rpc(Session, delete_table, [Tab]),
    ok = rpc(Session, create_table, [Tab, ["title"], [{type, rocksdb},
						     %%{ttl, 3000},
						      {num_of_shards, 12},
						      {distributed, false},
						      {hashing_method, uniform},
						      {data_model, array},
						      {comparator, ascending}]]),
    %IndexOn = [ "auxiliary_text","category","content_model","coordinates",
%		"defaultsort","external_link","heading","incoming_links",
%		"language","namespace","namespace_text","opening_text",
%		"outgoing_link","popularity_score","redirect","source_text",
%		"template","text","text_bytes","timestamp","version",
%		"version_type","wiki","wikibase_item"],
    IndexOn = ["text"],
    ok = rpc(Session, add_index, [Tab, IndexOn]),
    disconnect(Session),
    SPid = spawn(?MODULE, server, [Dir, Filenames, N, 0, 0]),
    [spawn(?MODULE, client, [SPid, Connection, Tab] ) || _ <- lists:seq(1,N)],
    _MonitorRef = erlang:monitor(process, SPid),
    register(my_server, SPid),
    {ok, SPid}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect(Node) when is_atom(Node) ->
    {ok, Node};
connect({Host, Port, User, Pass}) ->
    pbpc:connect(Host, Port, User, Pass).

disconnect(Node) when is_atom(Node)->
    ok;
disconnect(Session) when is_pid(Session) ->
    pbpc:disconnect(Session).

server(_Dir, _Filenames, 0, Success, Fail) ->
   io:format("[~p:~p] Success: ~p Fail: ~p~nServer stopping..~n",[?MODULE,?LINE, Success, Fail]);
server(Dir, Filenames, N, S, F) ->
    receive
	stop ->
	    server(Dir, [], N, S, F);
	{write_result, ok} ->
	    server(Dir, Filenames, N, S+?CHUNK, F);
	{write_result, {error, _}} ->
	    server(Dir, Filenames, N, S, F+?CHUNK);
	{client, Pid, register} ->
	    erlang:monitor(process, Pid),
	    server(Dir, Filenames, N, S, F);
	{client, Pid, file} ->
	    case Filenames of
		[H | T] ->
		    Pid ! {server, file, filename:join(Dir, H)},
		    server(Dir, T, N, S, F);
		[] ->
		    Pid ! {server, stop},
		    server(Dir, [], N, S, F)
	    end;
	{'DOWN', _MonitorRef, process, _Object, _Info} ->
	    server(Dir, Filenames, N-1, S, F)
    end.

client(SPid, Connection, Tab) ->
    SPid ! {client, self(), register},
    {ok, Session} = connect(Connection),
    client_loop(SPid, Session, Tab).

client_loop(SPid, Session, Tab) ->
    Start = os:timestamp(),
    SPid ! {client, self(), file},
    receive
	{server, file, File} ->
	    ok = load_file(SPid, Session, Tab, File),
	    Stop = os:timestamp(),
	    Time = timer:now_diff(Stop, Start)/1000/1000,
	    io:format("~p ~p took ~.3f~n",[self(), File, Time]),
	    client_loop(SPid, Session, Tab);
	{server, stop} ->
	    ok
    end.

load_file(SPid, Session, Tab, Filename) ->
    case file:read_file(Filename)  of
	{error, Reason} ->
	%%io:format("[~p:~p] read file error ~p~n",[?MODULE, ?LINE, Reason]),
	    {error, {Filename, file:format_error(Reason)}};
	{ok, Binary} ->
	    decode_loop(SPid, Session, Tab, Binary, [], 0)
    end.

decode_loop(SPid, Session, Tab, Binary, Auc, ?CHUNK) ->
    write_terms(SPid, Session, Auc),
    decode_loop(SPid, Session, Tab, Binary, [], 0);

decode_loop(SPid, Session, Tab, Binary, Auc, It) ->
    case jiffy:decode(Binary, [{null_term, undefined},
			       return_trailer]) of
	{has_trailer, Term, RestData} ->
	    NewAuc = add_term(Auc, Tab, Term),
	    decode_loop(SPid, Session, Tab, RestData, NewAuc, It+1);
	Term ->
	    NewAuc = add_term(Auc, Tab, Term),
	    write_terms(SPid, Session, NewAuc),
	    ok
    end.

write_term(SPid, Session, Tab, Term) ->
    %io:format("[~p:~p] Rest length: ~p~n", [?MODULE, ?LINE, length(Rest)]),
    case my_fold(Term) of
	{Key, Value} ->
	    Res = rpc(Session, write, [Tab, Key, Value]),
	    SPid ! {write_result, Res};
	skip ->
	    ok
    end.

add_term(Auc, Tab, Term) ->
    case my_fold(Term) of
	{Key, Value} ->
	    [{Tab, Key, Value} | Auc];
	skip ->
	    Auc
    end.

write_terms(SPid, Session, Terms) ->
    %Start = os:timestamp(),
    Res = rpc(Session, write, [Terms]),
    %Res = [ok],
    %Stop = os:timestamp(),
    %io:format("write_terms ~p ~.3f~n", [self(), timer:now_diff(Stop, Start)/1000/1000]),
    SPid ! {write_result, get_res(lists:usort(Res))}.

get_res([ok]) ->
    ok;
get_res(_) ->
    {error, failed}.

my_fold({List}) ->
    my_fold(List, undefined, []).
my_fold([{<<"title">>, TitleBin} | Rest], _K, V) ->
    my_fold(Rest, [{"title", binary_to_list(TitleBin)}],
		  [{"title", format_value(TitleBin)} | V]);
my_fold([{FBin, <<>>} | Rest], K, V) ->
    my_fold(Rest, K, [{binary_to_list(FBin), ""} | V]);
my_fold([{FBin, FVal} | Rest], K, V) ->
    my_fold(Rest, K, [{binary_to_list(FBin), format_value(FVal)} | V]);
my_fold([], undefined, _V) ->
    skip;
my_fold([], K, V) ->
    {K, V}.

format_value(Value) when is_binary(Value) ->
    lists:filter(fun(E) -> ((E<128) or (E>159)) end, binary_to_list(Value));
format_value(Value) when is_integer(Value) ->
    integer_to_list(Value);
format_value(Value) when is_float(Value) ->
    float_to_list(Value);
format_value(Value) when is_atom(Value) ->
    atom_to_list(Value);
format_value(undefined) ->
    "";
format_value({Value}) ->
    format_value(Value);
format_value({E1, E2}) ->
    lists:concat(["{", format_value(E1), ": ", format_value(E2), "}"]);
format_value(List) when is_list(List)  ->
    format_list(List, []).

format_list([Elem | Rest], Acc) ->
    format_list(Rest, [format_value(Elem) | Acc]);
format_list([], Acc) ->
    lists:reverse(Acc).

rpc(Node, Fun, Args) when is_atom(Node)->
    rpc:call(Node, enterdb, Fun, Args);
rpc(Session, Fun, Args) ->
    apply(pbpc, Fun, [Session | Args]).

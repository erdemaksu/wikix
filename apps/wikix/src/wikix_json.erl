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
-export([server/3,
	 client/3]).

-include_lib("gb_log/include/gb_log.hrl").

%%%===================================================================
%%% API functions
%%%===================================================================
import(Dir, N) ->
    {ok, Session} = pbpc:connect("127.0.0.1", 8887, "admin", "admin"),
    import(Session, Dir, N),
    ok = pbpc:disconnect(Session).

-spec import(Session :: pid(),
	     Dir :: string(),
	     N :: integer() | undefined) ->
    ok.
import(Session, Dir, N) ->
    {ok, Filenames} = file:list_dir(Dir),
    %io:format("[~p:~p] Filenames: ~p~n", [?MODULE, ?LINE, Filenames]),

    Tab = filename:basename(Dir),
    pbpc:delete_table(Session, Tab),
    ok = pbpc:create_table(Session, Tab, ["title"],
			   [{type, rocksdb},
			    %%{ttl, 3000},
			    {num_of_shards, 8},
			    {hashing_method, uniform},
			    {data_model, array},
			    {comparator, ascending}]),
    IndexOn = ["popularity_score","incoming_links","redirect","defaultsort",
	       "auxiliary_text","opening_text","heading","language",
	       "text_bytes","source_text","text","template","outgoing_link",
	       "external_link","category","timestamp","namespace_text",
	       "namespace","wiki","version","version_type","wikibase_item"],
    ok = pbpc:add_index(Session, Tab, IndexOn),
    SPid = spawn(?MODULE, server, [Dir, Filenames, N]),
    [spawn(?MODULE, client, [SPid, Session, Tab] ) || _ <- lists:seq(1,N)].

%%%===================================================================
%%% Internal functions
%%%===================================================================
server(_Dir, _Filenames, 0) ->
    ok;
server(Dir, Filenames, N) ->
    receive
	{client, Pid, register} ->
	    erlang:monitor(process, Pid),
	    server(Dir, Filenames, N);
	{client, Pid, file} ->
	    case Filenames of
		[H | T] ->
		    Pid ! {server, file, filename:join(Dir, H)},
		    server(Dir, T, N);
		[] ->
		    Pid ! {server, stop},
		    server(Dir, [], N)
	    end;
	{'DOWN', _MonitorRef, process, _Object, _Info} ->
	    server(Dir, Filenames, N-1)
    end.

client(SPid, Session, Tab) ->
    SPid ! {client, self(), register},
    client_loop(SPid, Session, Tab).

client_loop(SPid, Session, Tab) ->
    SPid ! {client, self(), file},
    receive
	{server, file, File} ->
	    ok = load_file(Session, Tab, File),
	    client_loop(SPid, Session, Tab);
	{server, stop} ->
	    ok
    end.

load_file(Session, Tab, Filename) ->
    %%io:format("[~p:~p] ~p is loading ~p~n", [?MODULE, ?LINE, self(), File]).
    case file:read_file(Filename)  of
	{error, Reason} ->
	    {error,{Filename, file:format_error(Reason)}};
	{ok, Binary} ->
	    Terms = decode_loop(Binary, []),
	    write_terms(Session, Tab, Terms)
    end.

decode_loop(Binary, Acc) ->
    case jiffy:decode(Binary, [{null_term, undefined},
			       return_trailer]) of
	{has_trailer, Term, RestData} ->
	    decode_loop(RestData, [Term | Acc]);
	Term ->
	    lists:reverse([Term | Acc])
    end.

write_terms(Session, Tab, [Term | Rest]) ->
    %io:format("[~p:~p] Rest length: ~p~n", [?MODULE, ?LINE, length(Rest)]),
    case my_fold(Term) of
	{Key, Value} ->
	    pbpc:write(Session, Tab, Key, Value);
	skip ->
	    ok
    end,
    write_terms(Session, Tab, Rest);
write_terms(_Session, _Tab, []) ->
    ok.

my_fold({List}) ->
    my_fold(List, undefined, []).
my_fold([{<<"title">>, TitleBin} | Rest], _K, V) ->
    my_fold(Rest, [{"title", binary_to_list(TitleBin)}], V);
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

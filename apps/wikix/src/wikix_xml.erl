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


-module(wikix_xml).


%% API functions
-export([test/1,
	 import/2,
	 import/3,
	 index/2,
	 index_on_node/1,
	 index_on_node_/1
	 ]).

-export([continuation/2,
	 event/2,
	 store/2]).

-include_lib("gb_log/include/gb_log.hrl").
-define(TABLE, "wiki").

%%%===================================================================
%%% API functions
%%%===================================================================
test(small)->
    import("/nvme/data/wiki_sm.xml", 5);
test(medium)->
    import("/nvme/data/wiki_md.xml", 10);
test(big)->
    import("/nvme/data/enwiki-20170401-pages-meta-current.xml", 5);
test(bigger)->
    import("/nvme/data/enwiki-20170401-pages-meta-current.xml", 1024);
test(biggest)-> %% 1859390996 Lines
    import("/nvme/data/enwiki-20170401-pages-meta-current.xml", undefined).

-spec index_on_node(Tab :: string()) ->
    {ok, integer()}.
index_on_node(Tab) ->
    {Module, Binary, File} = code:get_object_code(?MODULE),
    rpc:call(pundun97ae64@sitting, code, load_binary, [Module, File, Binary]),
    rpc:call(pundun97ae64@sitting, wikix, index_on_node_, [Tab], infinity).

index_on_node_(Tab) ->
    enterdb:delete_table(Tab),
    enterdb:create_table(Tab, ["title"], [{type, rocksdb},{num_of_shards, 8},
					  {comparator, ascending}]),
    ok = enterdb:add_index(Tab, ["text"]),
    case enterdb:first(?TABLE) of
	{ok, KV, It} ->
	    index_loop_on_node(Tab, KV, It, 0);
	{error,invalid} ->
	    {ok, 0}
    end.

index_loop_on_node(Tab, {Key, Val}, It, Count) ->
    case Count rem 10000 of
	0 -> io:format("~p.",[Count]);
	_ -> ok
    end,
    spawn(fun() -> enterdb:write(Tab, Key, Val) end),
    case enterdb:next(It) of
	{ok, KV} ->
	    index_loop_on_node(Tab, KV, It, Count+1);
	{error,invalid} ->
	    {ok, Count+1}
    end.

-spec index(Session:: pid(), Tab :: string()) ->
    {ok, integer()}.
index(Session, Tab) ->
    %%pbpc:delete_table(Session, Tab),
    rpc:call(pundun97ae64@sitting, enterdb, delete_table, [Tab]),
    %%ok = pbpc:create_table(Session, Tab, ["title"],
%%			    [{type, rocksdb},
%%			     {num_of_shards, 8},
%%			     {comparator, ascending}]),
    rpc:call(pundun97ae64@sitting, enterdb, create_table, [Tab, ["title"],
							   [{type, rocksdb},
							    {num_of_shards, 8},
							    {comparator, ascending}]]),
    %ok = pbpc:add_index(Session, Tab, ["text"]),
    %%case pbpc:first(Session, ?TABLE) of
    case rpc:call(pundun97ae64@sitting, enterdb, first, [?TABLE]) of
	{ok, KV, It} ->
	    index_loop(Session, Tab, KV, It, 0);
	%%{error,{system,"{error,invalid}"}} ->
	{error,invalid} ->
	    {ok, 0}
    end.

index_loop(Session, Tab, {Key, Val}, It, Count) ->
    case Count rem 10000 of
	0 -> io:format("~p.",[Count]);
	_ -> ok
    end,
    %%pbpc:write(Session, Tab, Key, Val),
    rpc:call(pundun97ae64@sitting, enterdb, write, [Tab, Key, Val]),
    %%case pbpc:next(Session, It) of
    case rpc:call(pundun97ae64@sitting, enterdb, next, [It]) of
	{ok, KV} ->
	    index_loop(Session, Tab, KV, It, Count+1);
	%%{error,{system,"{error,invalid}"}} ->
	{error,invalid} ->
	    {ok, Count+1}
    end.

import(Filename, N) ->
    {ok, Session} = pbpc:connect("127.0.0.1", 8887, "admin", "admin"),
    import(Session, Filename, N),
    ok = pbpc:disconnect(Session).

-spec import(Session :: pid(),
	     Filename :: string(),
	     N :: integer() | undefined) ->
    ok.
import(Session, Filename, N) ->
    case file:open(Filename, [raw, read,binary])  of
	{error, Reason} ->
	    {error,{Filename, file:format_error(Reason)}};
	{ok, FD} ->
	    pbpc:delete_table(Session, ?TABLE),
	    ok = pbpc:create_table(Session, ?TABLE, ["title"],
				   [{type, rocksdb},
				    %%{ttl, 3000},
				    {num_of_shards, 8},
				    {hashing_method, uniform},
				    {data_model, array},
				    {comparator, ascending}]),
	    %%ok = pbpc:add_index(Session, ?TABLE, ["text"]),
	    Acc = #{pages => 0,
		    max_pages => N,
		    data => #{},
		    tags => [],
		    session => Session},
	    State = {FD, 0, 4096000},
	    Res = (catch erlsom:parse_sax(<<>>, Acc, fun event/2,
		[{continuation_function, fun continuation/2, State},
		 {max_entity_size, infinity},
		 {max_expanded_entity_size, infinity}])),
	    timer:sleep(1000),
	    Res
    end.

continuation(Tail, {Handle, Offset, Chunk}) ->
    case file:pread(Handle, Offset, Chunk) of
        {ok, Data} ->
	    {<<Tail/binary, Data/binary>>, {Handle, Offset + Chunk, Chunk}};
	eof ->
	    {Tail, {Handle, Offset, Chunk}}
    end.

event({startElement, _Uri, "page", _, _}, State = #{tags := Tags}) ->
    State#{tags => ["page" | Tags]};
event({endElement, _Uri, "page", _}, State = #{tags := ["page" | Rest],
					       data := Data,
					       pages := P,
					       max_pages := undefined,
					       session := Session}) ->
    case P rem 10000 of
	0 -> io:format("~p.",[P]);
	_ -> ok
    end,
    erlang:spawn(?MODULE, store, [Session, Data]),
    %store(Session, Data),
    State#{tags => Rest, pages => P+1, data => #{}};
event({endElement, _Uri, "page", _}, State = #{tags := ["page" | Rest],
					       data := Data,
					       pages := P,
					       max_pages := N,
					       session := Session}) ->
    erlang:spawn(?MODULE, store, [Session, Data]),
    %store(Session, Data),
    Sum = P+1,
    case Sum rem 10000 of
	0 -> io:format("~p.",[Sum]);
	_ -> ok
    end,
    EndState = State#{tags => Rest, pages => Sum, data => #{}},
    case Sum of
	N -> throw( {stop, "max_pages_reached", EndState} );
	_ -> EndState
    end;
event({startElement, _Uri, Elem, _, _}, State = #{tags := Tags}) ->
    State#{tags => [Elem | Tags]};
event({endElement, _Uri, Elem, _}, State = #{tags := [Elem | Rest]}) ->
    State#{tags => Rest};
event({characters, String}, State = #{tags := [Elem | _],
					    data := Data}) ->
    Base = maps:get(Elem, Data, ""),
    State#{data => Data#{Elem => string:concat(Base, [C || C <- String, C =< 255])}};
event(_Event, State) ->
    State.

store(Session, Data = #{"title" := Title}) ->
    Key = [{"title", Title}],
    Value = maps:fold(fun("title", _, Acc) -> Acc;
			 (K, V, Acc) -> [{K, V} | Acc] end,
		      [],  Data),
    pbpc:write(Session, ?TABLE, Key, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

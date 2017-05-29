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


-module(wikix).


%% API functions
-export([test/1,
	 import/2,
	 import/3]).

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

import(Filename, N) ->
    {ok, Session} = pbpc:connect("127.0.0.1", 8887, "admin", "admin"),
    import(Session, Filename, N),
    ok = pbpc:disconnect(Session).

-spec import(Filename :: string(), N :: integer() | undefined) ->
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

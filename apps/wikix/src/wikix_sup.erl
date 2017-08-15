%%%-------------------------------------------------------------------
%% @doc wikix top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(wikix_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    gb_beam_autoloader:start(),
    gb_log_oam:load_store_filters_beam(),
    gb_log_oam:load_default_filter(),
    {ok, { {one_for_all, 0, 1}, []} }.

%%====================================================================
%% Internal functions
%%====================================================================

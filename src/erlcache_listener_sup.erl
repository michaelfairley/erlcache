-module(erlcache_listener_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok, ListenSocket} = gen_tcp:listen(11211, [{active, false}]),
    spawn_link(fun empty_listeners/0),
    {ok, {{simple_one_for_one, 5, 10},
	  [{listener, {erlcache_listener, start_link, [ListenSocket]},
	    temporary, 1000, worker, [erlcache_listener]}]}}.

start_socket() ->
    supervisor:start_child(?MODULE, []).

empty_listeners() ->
    [start_socket() || _ <- lists:seq(1,20)],
    ok.

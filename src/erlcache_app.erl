-module(erlcache_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, _} = ranch:start_listener(memcached_listener, 10,
				   ranch_tcp, [{port, 11211}], erlcache_protocol, []),
    erlcache_sup:start_link().

stop(_State) ->
    ok.

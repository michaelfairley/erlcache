-module(erlcache_cache).

-behaviour(gen_server).

%% API
-export([start_link/0, set/4, get/1, flush/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {kv}).
-record(item, {value, flags}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

set(Key, Value, Expiration, Flags) ->
    gen_server:call(?MODULE, {set, Key, Value, Expiration, Flags}).
get(Key) ->
    gen_server:call(?MODULE, {get, Key}).
flush() ->
    gen_server:call(?MODULE, {flush}).

init([]) ->
    {ok, #state{kv=dict:new()}}.

handle_call({set, Key, Value, _Expiration, Flags}, _From, #state{kv=KV}) ->
    NewKV = dict:store(Key, #item{value=Value, flags=Flags}, KV),
    {reply, ok, #state{kv=NewKV}};
handle_call({get, Key}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, #item{value=Value, flags=Flags}} ->
	    FlagBin = <<Flags:32>>,
	    {reply, {ok, Value, FlagBin}, #state{kv=KV}};
	error ->
	    {reply, notfound, #state{kv=KV}}
    end;
handle_call({flush}, _From, #state{kv=_KV}) ->
    {reply, {ok}, #state{kv=dict:new()}};
handle_call(_Request, _From, State) ->
    Reply = fellthrough,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

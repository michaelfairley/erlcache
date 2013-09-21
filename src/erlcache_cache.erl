-module(erlcache_cache).

-behaviour(gen_server).

%% API
-export([start_link/0, set/5, add/5, replace/5, get/1, flush/0, delete/1, incr/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {kv}).
-record(item, {value, flags, cas}).

-define(INCRDECR_NO_CREATE, 16#ffffffff).
-define(EMPTY_CAS, 0).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

set(Key, Value, Expiration, Flags, CAS) ->
    gen_server:call(?MODULE, {set, Key, Value, Expiration, Flags, CAS}).
add(Key, Value, Expiration, Flags, CAS) ->
    gen_server:call(?MODULE, {add, Key, Value, Expiration, Flags, CAS}).
replace(Key, Value, Expiration, Flags, CAS) ->
    gen_server:call(?MODULE, {replace, Key, Value, Expiration, Flags, CAS}).
get(Key) ->
    gen_server:call(?MODULE, {get, Key}).
flush() ->
    gen_server:call(?MODULE, {flush}).
delete(Key) ->
    gen_server:call(?MODULE, {delete, Key}).
incr(Key, Amount, Initial, Expiration) ->
    gen_server:call(?MODULE, {incr, Key, Amount, Initial, Expiration}).

init([]) ->
    {ok, #state{kv=dict:new()}}.

handle_call({set, Key, Value, _Expiration, Flags, CAS}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, #item{cas=OldCAS}} ->
	    case CAS of
		?EMPTY_CAS ->
		    NewKV = dict:store(Key, #item{value=Value, flags=Flags, cas=1}, KV),
		    {reply, ok, #state{kv=NewKV}};
		OldCAS ->

		    NewKV = dict:store(Key, #item{value=Value, flags=Flags, cas=CAS+1}, KV),
		    {reply, ok, #state{kv=NewKV}};
		_ ->
		    {reply, key_exists, #state{kv=KV}}
	    end;
	error ->
	    case CAS of
		?EMPTY_CAS ->
		    NewKV = dict:store(Key, #item{value=Value, flags=Flags, cas=1}, KV),
		    {reply, ok, #state{kv=NewKV}};
		_ ->
		    {reply, not_found, #state{kv=KV}}
	    end
    end;
handle_call({add, Key, Value, _Expiration, Flags, _CAS}, _From, #state{kv=KV}) ->
    case dict:is_key(Key, KV) of
	true ->
	    {reply, key_exists, #state{kv=KV}};
	false ->
	    NewKV = dict:store(Key, #item{value=Value, flags=Flags, cas=1}, KV),
	    {reply, ok, #state{kv=NewKV}}
    end;
handle_call({replace, Key, Value, _Expiration, Flags, _CAS}, _From, #state{kv=KV}) ->
    case dict:is_key(Key, KV) of
	true ->
	    NewKV = dict:store(Key, #item{value=Value, flags=Flags, cas=1}, KV),
	    {reply, ok, #state{kv=NewKV}};
	false ->
	    {reply, not_found, #state{kv=KV}}
    end;
handle_call({get, Key}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, #item{value=Value, flags=Flags, cas=CAS}} ->
	    FlagBin = <<Flags:32>>,
	    {reply, {ok, Value, FlagBin, CAS}, #state{kv=KV}};
	error ->
	    {reply, not_found, #state{kv=KV}}
    end;
handle_call({flush}, _From, #state{kv=_KV}) ->
    {reply, {ok}, #state{kv=dict:new()}};
handle_call({delete, Key}, _From, #state{kv=KV}) ->
    NewKV = dict:erase(Key, KV),
    {reply, {ok}, #state{kv=NewKV}};
handle_call({incr, Key, Amount, Initial, Expiration}, _From, #state{kv=KV}) ->
    case dict:is_key(Key, KV) of
	true ->
	    NewVal = max(dict:fetch(Key, KV) + Amount, 0),
	    NewKV = dict:store(Key, NewVal, KV),
	    {reply, {ok, NewVal}, #state{kv=NewKV}};
	false ->
	    if
		Expiration == ?INCRDECR_NO_CREATE ->
		    {reply, not_found, #state{kv=KV}};
		true ->
		    NewKV = dict:store(Key, Initial, KV),
		    {reply, {ok, Initial}, #state{kv=NewKV}}
	    end
    end;
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

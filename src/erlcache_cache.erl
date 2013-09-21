-module(erlcache_cache).

-behaviour(gen_server).

%% API
-export([start_link/0, set/5, add/5, replace/5, get/1, flush/0, delete/2, incr/4, append/3, prepend/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(INCRDECR_NO_CREATE, 16#ffffffff).
-define(EMPTY_CAS, 0).
-define(NEW_CAS, 1).

-record(state, {kv}).
-record(item, {value, flags=0, cas=?NEW_CAS}).


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
delete(Key, CAS) ->
    gen_server:call(?MODULE, {delete, Key, CAS}).
incr(Key, Amount, Initial, Expiration) ->
    gen_server:call(?MODULE, {incr, Key, Amount, Initial, Expiration}).
append(Key, Value, CAS) ->
    gen_server:call(?MODULE, {append, Key, Value, CAS}).
prepend(Key, Value, CAS) ->
    gen_server:call(?MODULE, {prepend, Key, Value, CAS}).

init([]) ->
    {ok, #state{kv=dict:new()}}.

handle_call({set, Key, Value, _Expiration, Flags, CAS}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, #item{cas=OldCAS}} ->
	    case CAS of
		?EMPTY_CAS ->
		    NewKV = dict:store(Key, #item{value=Value, flags=Flags}, KV),
		    {reply, {ok, ?NEW_CAS}, #state{kv=NewKV}};
		OldCAS ->
		    NewCAS = CAS+1,
		    NewKV = dict:store(Key, #item{value=Value, flags=Flags, cas=NewCAS}, KV),
		    {reply, {ok, ?NEW_CAS}, #state{kv=NewKV}};
		_ ->
		    {reply, {key_exists, OldCAS}, #state{kv=KV}}
	    end;
	error ->
	    case CAS of
		?EMPTY_CAS ->
		    NewKV = dict:store(Key, #item{value=Value, flags=Flags}, KV),
		    {reply, {ok, ?NEW_CAS}, #state{kv=NewKV}};
		_ ->
		    {reply, {not_found, ?EMPTY_CAS}, #state{kv=KV}}
	    end
    end;
handle_call({add, Key, Value, _Expiration, Flags, _CAS}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, #item{cas=OldCAS}} ->
	    {reply, {key_exists, OldCAS}, #state{kv=KV}};
	error ->
	    NewKV = dict:store(Key, #item{value=Value, flags=Flags}, KV),
	    {reply, {ok, ?NEW_CAS}, #state{kv=NewKV}}
    end;
handle_call({replace, Key, Value, _Expiration, Flags, _CAS}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, _Item} ->
	    NewKV = dict:store(Key, #item{value=Value, flags=Flags}, KV),
	    {reply, {ok, ?NEW_CAS}, #state{kv=NewKV}};
	error ->
	    {reply, {not_found, ?EMPTY_CAS}, #state{kv=KV}}
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
handle_call({delete, Key, CAS}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, #item{cas=CAS}} ->
	    NewKV = dict:erase(Key, KV),
	    {reply, ok, #state{kv=NewKV}};
	error ->
	    {reply, ok, #state{kv=KV}};
	_ ->
	    case CAS of
		?EMPTY_CAS ->
		    NewKV = dict:erase(Key, KV),
		    {reply, ok, #state{kv=NewKV}};
		_ ->
		    {reply, key_exists, #state{kv=KV}}
	    end
    end;
handle_call({incr, Key, Amount, Initial, Expiration}, _From, #state{kv=KV}) ->
    case dict:is_key(Key, KV) of
	true ->
	    #item{value=OldVal} = dict:fetch(Key, KV),
	    NewVal = max(binary_to_int(OldVal) + Amount, 0),
	    NewKV = dict:store(Key, #item{value=int_to_binary(NewVal)}, KV),
	    {reply, {ok, NewVal, ?NEW_CAS}, #state{kv=NewKV}};
	false ->
	    if
		Expiration == ?INCRDECR_NO_CREATE ->
		    {reply, not_found, #state{kv=KV}};
		true ->
		    NewKV = dict:store(Key, #item{value=int_to_binary(Initial)}, KV),
		    {reply, {ok, Initial, ?NEW_CAS}, #state{kv=NewKV}}
	    end
    end;
handle_call({append, Key, Value, CAS}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, #item{value=OriginalValue, flags=Flags}} when CAS == ?EMPTY_CAS->
	    NewValue = <<OriginalValue/binary, Value/binary>>,
	    NewKV = dict:store(Key, #item{value=NewValue, flags=Flags}, KV),
	    {reply, ok, #state{kv=NewKV}};
	{ok, #item{value=OriginalValue, flags=Flags, cas=CAS}} ->
	    NewValue = <<OriginalValue/binary, Value/binary>>,
	    NewKV = dict:store(Key, #item{value=NewValue, flags=Flags, cas=CAS+1}, KV),
	    {reply, ok, #state{kv=NewKV}};
	error ->
	    {reply, not_found, #state{kv=KV}};
	_ ->
	    {reply, key_exists, #state{kv=KV}}
    end;
handle_call({prepend, Key, Value, CAS}, _From, #state{kv=KV}) ->
    case dict:find(Key, KV) of
	{ok, #item{value=OriginalValue, flags=Flags}} when CAS == ?EMPTY_CAS->
	    NewValue = <<Value/binary, OriginalValue/binary>>,
	    NewKV = dict:store(Key, #item{value=NewValue, flags=Flags}, KV),
	    {reply, ok, #state{kv=NewKV}};
	{ok, #item{value=OriginalValue, flags=Flags, cas=CAS}} ->
	    NewValue = <<Value/binary, OriginalValue/binary>>,
	    NewKV = dict:store(Key, #item{value=NewValue, flags=Flags, cas=CAS+1}, KV),
	    {reply, ok, #state{kv=NewKV}};
	error ->
	    {reply, not_found, #state{kv=KV}};
	_ ->
	    {reply, key_exists, #state{kv=KV}}
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

binary_to_int(Bin) ->
    binary_to_int(Bin, 0).
binary_to_int(<<>>, Acc) ->
    Acc;
binary_to_int(<<Digit:8, Rest/binary>>, Acc) ->
    binary_to_int(Rest, Acc * 10 + (Digit - $0)).

int_to_binary(Num) ->
    int_to_binary(Num, <<>>).
int_to_binary(0, Bin) ->
    Bin;
int_to_binary(Num, Bin) ->
    Char = (Num rem 10) + $0,
    int_to_binary(Num div 10, <<Char:8, Bin/binary>>).

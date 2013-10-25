-module(erlcache_cache).

-behaviour(gen_server).

%% API
-export([start_link/0, set/5, add/5, replace/5, get/1, flush/0, delete/2, incr/4, append/3, prepend/3, stat/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(INCRDECR_NO_CREATE, 16#ffffffff).
-define(EMPTY_CAS, 0).
-define(NEW_CAS, 1).

-record(state, {kv, stats}).
-record(item, {value, flags=0, cas=?NEW_CAS, expiration=infinity}).


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
stat() ->
    gen_server:call(?MODULE, {stat}).

init([]) ->
    Stats = dict:new(),
    Stats1 = dict:store(get_hits, <<"0">>, Stats),
    Stats2 = dict:store(cmd_flush, <<"0">>, Stats1),
    {ok, #state{kv=dict:new(), stats=Stats2}}.

handle_call({set, Key, Value, _Expiration, _Flags, _CAS}, _From, State) when size(Value) > 1024*1024 ->
    NewKV = dict:erase(Key, State#state.kv),
    {reply, too_large, State#state{kv=NewKV}};
handle_call({set, Key, Value, Expiration, Flags, CAS}, _From, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, #item{cas=OldCAS}} ->
	    case CAS of
		?EMPTY_CAS ->
		    NewKV = dict:store(Key, #item{value=Value, flags=Flags}, State#state.kv),
		    {reply, {ok, ?NEW_CAS}, State#state{kv=NewKV}};
		OldCAS ->
		    NewCAS = CAS+1,
		    NewKV = dict:store(Key, #item{value=Value, flags=Flags, cas=NewCAS}, State#state.kv),
		    {reply, {ok, NewCAS}, State#state{kv=NewKV}};
		_ ->
		    {reply, {key_exists, OldCAS}, State}
	    end;
	error ->
	    case CAS of
		?EMPTY_CAS ->
		    NewKV = dict:store(Key, #item{value=Value, flags=Flags, expiration=expires_in(Expiration)}, State#state.kv),
		    {reply, {ok, ?NEW_CAS}, State#state{kv=NewKV}};
		_ ->
		    {reply, {not_found, ?EMPTY_CAS}, State}
	    end
    end;
handle_call({add, Key, Value, _Expiration, Flags, _CAS}, _From, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, #item{cas=OldCAS}} ->
	    {reply, {key_exists, OldCAS}, State};
	error ->
	    NewKV = dict:store(Key, #item{value=Value, flags=Flags}, State#state.kv),
	    {reply, {ok, ?NEW_CAS}, State#state{kv=NewKV}}
    end;
handle_call({replace, Key, Value, _Expiration, Flags, _CAS}, _From, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, _Item} ->
	    NewKV = dict:store(Key, #item{value=Value, flags=Flags}, State#state.kv),
	    {reply, {ok, ?NEW_CAS}, State#state{kv=NewKV}};
	error ->
	    {reply, {not_found, ?EMPTY_CAS}, State}
    end;
handle_call({get, Key}, _From, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, #item{value=Value, flags=Flags, cas=CAS, expiration=Expiration}} ->
	    case is_expired(Expiration) of
		true ->
		    {reply, not_found, State};
		false ->
		    FlagBin = <<Flags:32>>,
		    {reply, {ok, Value, FlagBin, CAS}, State}
	    end;
	error ->
	    {reply, not_found, State}
    end;
handle_call({flush}, _From, #state{kv=_KV, stats=Stats}) ->
    NewStats = dict:update(cmd_flush, incr_binary_fun(1), Stats),
    {reply, {ok}, #state{kv=dict:new(), stats=NewStats}};
handle_call({delete, Key, CAS}, _From, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, #item{cas=CAS}} ->
	    NewKV = dict:erase(Key, State#state.kv),
	    {reply, ok, State#state{kv=NewKV}};
	error ->
	    {reply, ok, State};
	_ ->
	    case CAS of
		?EMPTY_CAS ->
		    NewKV = dict:erase(Key, State#state.kv),
		    {reply, ok, State#state{kv=NewKV}};
		_ ->
		    {reply, key_exists, State}
	    end
    end;
handle_call({incr, Key, Amount, Initial, Expiration}, _From, State) ->
    case dict:is_key(Key, State#state.kv) of
	true ->
	    #item{value=OldVal, cas=CAS} = dict:fetch(Key, State#state.kv),
	    try binary_to_integer(OldVal) of
		OldValInt ->
		    NewVal = max(OldValInt + Amount, 0),
		    NewCAS = CAS+1,
		    NewKV = dict:store(Key, #item{value=integer_to_binary(NewVal), cas=NewCAS}, State#state.kv),
		    {reply, {ok, NewVal, NewCAS}, State#state{kv=NewKV}}
	    catch
		error:badarg ->
		    {reply, non_numeric, State}
	    end;
	false ->
	    if
		Expiration == ?INCRDECR_NO_CREATE ->
		    {reply, not_found, State};
		true ->
		    NewKV = dict:store(Key, #item{value=integer_to_binary(Initial)}, State#state.kv),
		    {reply, {ok, Initial, ?NEW_CAS}, State#state{kv=NewKV}}
	    end
    end;
handle_call({append, Key, Value, CAS}, _From, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, #item{value=OriginalValue, flags=Flags}} when CAS == ?EMPTY_CAS->
	    NewValue = <<OriginalValue/binary, Value/binary>>,
	    NewKV = dict:store(Key, #item{value=NewValue, flags=Flags}, State#state.kv),
	    {reply, ok, State#state{kv=NewKV}};
	{ok, #item{value=OriginalValue, flags=Flags, cas=CAS}} ->
	    NewValue = <<OriginalValue/binary, Value/binary>>,
	    NewKV = dict:store(Key, #item{value=NewValue, flags=Flags, cas=CAS+1}, State#state.kv),
	    {reply, ok, State#state{kv=NewKV}};
	error ->
	    {reply, not_stored, State};
	_ ->
	    {reply, key_exists, State}
    end;
handle_call({prepend, Key, Value, CAS}, _From, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, #item{value=OriginalValue, flags=Flags}} when CAS == ?EMPTY_CAS->
	    NewValue = <<Value/binary, OriginalValue/binary>>,
	    NewKV = dict:store(Key, #item{value=NewValue, flags=Flags}, State#state.kv),
	    {reply, ok, State#state{kv=NewKV}};
	{ok, #item{value=OriginalValue, flags=Flags, cas=CAS}} ->
	    NewValue = <<Value/binary, OriginalValue/binary>>,
	    NewKV = dict:store(Key, #item{value=NewValue, flags=Flags, cas=CAS+1}, State#state.kv),
	    {reply, ok, State#state{kv=NewKV}};
	error ->
	    {reply, not_stored, State};
	_ ->
	    {reply, key_exists, State}
    end;
handle_call({stat}, _From, State) ->
    {reply, State#state.stats, State};
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

incr_binary_fun(Amount) ->
    fun(BinaryBefore) ->
	    IntBefore = binary_to_integer(BinaryBefore),
	    IntAfter = IntBefore + Amount,
	    integer_to_binary(IntAfter)
    end.

expires_in(0) ->
    infinity;
expires_in(Delta) ->
    now_int() + Delta*1000*1000.

is_expired(infinity) ->
    false;
is_expired(Time) ->
    now_int() > Time.

now_int() ->
    {Mega, Secs, Micro} = now(),
    Mega*1000*1000*1000*1000 + Secs*1000*1000 + Micro.

-module(erlcache_protocol).
-include("erlcache_constants.hrl").

-export([start_link/4]).
-export([init/4]).

-record(response, {
	  status,
	  body = <<>>,
	  key = <<>>,
	  extras = <<>>,
	  cas = 0
	 }).

-define(TIMEOUT, 5000).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
        {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, []).

recv(Transport, Socket, Length) ->
    if
	Length > 0 ->
	    {ok, Data} = Transport:recv(Socket, Length, ?TIMEOUT),
	    Data;
	true ->
	    <<>>
    end.

loop(Socket, Transport, Queue) ->
    case Transport:recv(Socket, ?HEADER_LENGTH, ?TIMEOUT) of
        {ok, Data} ->
	    <<16#80:8,
	      Opcode:8,
	      KeyLength:16,
	      ExtrasLength:8,
	      0:8,
	      _Reserved:16,
	      TotalBodyLength:32,
	      Opaque:32,
	      CAS:64
	    >> = Data,

	    Extras = recv(Transport, Socket, ExtrasLength),
	    Key = recv(Transport, Socket, KeyLength),
	    BodyLength = TotalBodyLength - KeyLength - ExtrasLength,
	    Body = recv(Transport, Socket, BodyLength),

	    case handle_command(Opcode, Key, Body, Extras, CAS) of
		{reply, Response} ->
		    ResponseData = response_to_binary(Response, Opaque, Opcode),
		    Transport:send(Socket, lists:reverse(Queue)),
		    Transport:send(Socket, ResponseData),
		    loop(Socket, Transport, []);
		{reply_many, Responses} ->
		    ResponsesData = [response_to_binary(Response, Opaque, Opcode) || Response <- Responses],
		    Transport:send(Socket, lists:reverse(Queue)),
		    Transport:send(Socket, ResponsesData),
		    loop(Socket, Transport, []);
		{defer, Response} ->
		    ResponseData = response_to_binary(Response, Opaque, Opcode),
		    loop(Socket, Transport, [ResponseData|Queue]);
		no_response ->
		    loop(Socket, Transport, Queue)
	    end;
	_ ->
	    ok = Transport:close(Socket)
    end.

handle_command(?NOOP, _Key, _Body, _Extras, _CAS) ->
    {reply, #response{status=?SUCCESS}};
handle_command(?FLUSH, _Key, _Body, <<>>, _CAS) ->
    handle_command(?FLUSH, <<>>, <<>>, <<0:32>>, <<>>);
handle_command(?FLUSH, _Key, _Body, <<Expiration:32>>, _CAS) ->
    case Expiration of
	0 ->
	    erlcache_cache:flush();
	_ ->
	    timer:apply_after(Expiration, erlcache_cache, flush, [])
    end,
    {reply, #response{status=?SUCCESS}};
handle_command(?GET, Key, _Body, _Extras, _CAS) ->
    case erlcache_cache:get(Key) of
	{ok, Value, Flags, CAS} ->
	    {reply, #response{status=?SUCCESS, body=Value, extras=Flags, cas=CAS}};
	not_found ->
	    {reply, #response{status=?NOT_FOUND}}
    end;
handle_command(?GETQ, Key, _Body, _Extras, _CAS) ->
    case erlcache_cache:get(Key) of
	{ok, Value, Flags, CAS} ->
	    {defer, #response{status=?SUCCESS, body=Value, extras=Flags, cas=CAS}};
	not_found ->
	    no_response
    end;
handle_command(?GETKQ, Key, _Body, _Extras, _CAS) ->
    case erlcache_cache:get(Key) of
	{ok, Value, Flags, CAS} ->
	    {defer, #response{status=?SUCCESS, key=Key, body=Value, extras=Flags, cas=CAS}};
	not_found ->
	    no_response
    end;
handle_command(?DELETE, Key, _Body, _Extras, CAS) ->
    case erlcache_cache:delete(Key, CAS) of
	ok ->
	    {reply, #response{status=?SUCCESS}};
	key_exists ->
	    {reply, #response{status=?KEY_EXISTS}}
    end;
handle_command(?SET, Key, Value, Extras, CAS) ->
    <<Flags:32, Expiration:32>> = Extras,
    case erlcache_cache:set(Key, Value, Expiration, Flags, CAS) of
	{ok, NewCAS} ->
	    {reply, #response{status=?SUCCESS, cas=NewCAS}};
	{not_found, NewCAS} ->
	    {reply, #response{status=?NOT_FOUND, cas=NewCAS}};
	{key_exists, NewCAS} ->
	    {reply, #response{status=?KEY_EXISTS, cas=NewCAS}};
	too_large ->
	    {reply, #response{status=?TOO_LARGE}}
    end;
handle_command(?SETQ, Key, Value, Extras, CAS) ->
    case handle_command(?SET, Key, Value, Extras, CAS) of
	{reply, #response{status=?SUCCESS}} ->
	    no_response;
	Response ->
	    Response
    end;
handle_command(?ADD, Key, Value, Extras, CAS) ->
    <<Flags:32, Expiration:32>> = Extras,
    {Success, NewCAS} = erlcache_cache:add(Key, Value, Expiration, Flags, CAS),
    Status = case Success of
		 ok -> ?SUCCESS;
		 key_exists -> ?KEY_EXISTS
	     end,
    {reply, #response{status=Status, cas=NewCAS}};
handle_command(?REPLACE, Key, Value, Extras, CAS) ->
    <<Flags:32, Expiration:32>> = Extras,
    {Success, NewCAS} = erlcache_cache:replace(Key, Value, Expiration, Flags, CAS),
    Status = case Success of
		 ok -> ?SUCCESS;
		 not_found -> ?NOT_FOUND
	     end,
    {reply, #response{status=Status, cas=NewCAS}};
handle_command(?VERSION, <<>>, <<>>, <<>>, _CAS) ->
    {reply, #response{status=?SUCCESS, body= <<"1.2.3">>}};
handle_command(?INCR, Key, <<>>, <<Amount:64, Initial:64, Expiration:32>>, _CAS) ->
    case erlcache_cache:incr(Key, Amount, Initial, Expiration) of
	{ok, Value, NewCAS} ->
	    {reply, #response{status=?SUCCESS, body= <<Value:64>>, cas=NewCAS}};
	not_found ->
	    {reply, #response{status=?NOT_FOUND}};
	non_numeric ->
	    {reply, #response{status=?NON_NUMERIC}}
    end;
handle_command(?DECR, Key, <<>>, <<Amount:64, Initial:64, Expiration:32>>, _CAS) ->
    case erlcache_cache:incr(Key, -Amount, Initial, Expiration) of
	{ok, Value, NewCAS} ->
	    {reply, #response{status=?SUCCESS, body= <<Value:64>>, cas=NewCAS}};
	not_found ->
	    {reply, #response{status=?NOT_FOUND}};
	non_numeric ->
	    {reply, #response{status=?NON_NUMERIC}}
    end;
handle_command(?APPEND, Key, Value, <<>>, CAS) ->
    case erlcache_cache:append(Key, Value, CAS) of
	ok ->
	    {reply, #response{status=?SUCCESS}};
	not_stored ->
	    {reply, #response{status=?NOT_STORED}};
	key_exists ->
	    {reply, #response{status=?KEY_EXISTS}}
    end;
handle_command(?PREPEND, Key, Value, <<>>, CAS) ->
    case erlcache_cache:prepend(Key, Value, CAS) of
	ok ->
	    {reply, #response{status=?SUCCESS}};
	not_stored ->
	    {reply, #response{status=?NOT_STORED}};
	key_exists ->
	    {reply, #response{status=?KEY_EXISTS}}
    end;
handle_command(?STAT, _Key, <<>>, <<>>, _CAS) ->
    Stats = erlcache_cache:stat(),
    Responses = dict:fold(fun(Key, Value, RespAcc) ->
		     [#response{status=?SUCCESS, key=atom_to_binary(Key, utf8), body=Value} | RespAcc]
	     end, [#response{status=?SUCCESS}], Stats),
    {reply_many, Responses};
handle_command(OpCode, _, _, _, _) ->
    io:format("No match: ~.16B~n", [OpCode]),
    {reply, #response{status=?UNKNOWN_COMMAND}}.

add_error_string(Response=#response{status=?SUCCESS}) ->
    Response;
add_error_string(Response=#response{status=?NOT_FOUND}) ->
    Response#response{body= <<"Not found">>};
add_error_string(Response=#response{status=?KEY_EXISTS}) ->
    Response#response{body= <<"Key exists">>};
add_error_string(Response=#response{status=?TOO_LARGE}) ->
    Response#response{body= <<"Too large">>};
add_error_string(Response=#response{status=?INVALID_ARGUMENTS}) ->
    Response#response{body= <<"Invalid arguments">>};
add_error_string(Response=#response{status=?NOT_STORED}) ->
    Response#response{body= <<"Not stored">>};
add_error_string(Response=#response{status=?NON_NUMERIC}) ->
    Response#response{body= <<"Non numeric">>};
add_error_string(Response=#response{status=?UNKNOWN_COMMAND}) ->
    Response#response{body= <<"Unknown command">>};
add_error_string(Response=#response{status=?OUT_OF_MEMORY}) ->
    Response#response{body= <<"Out of memory">>}.


response_to_binary(Response, Opaque, Opcode) ->
    #response{status=Status,
	      body=Body,
	      extras=Extras,
	      key=Key,
	      cas=CAS
	     } = add_error_string(Response),
    BodyLength = byte_size(Body),
    ExtrasLength = byte_size(Extras),
    KeyLength = byte_size(Key),
    TotalBodyLength = BodyLength + ExtrasLength + KeyLength,
    [<<16#81:8,
      Opcode,
      KeyLength:16,
      ExtrasLength:8,
      0:8,
      Status:16,
      TotalBodyLength:32,
      Opaque:32,
      CAS:64>>,
     Extras,
     Key,
     Body].

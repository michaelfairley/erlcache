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
handle_command(?FLUSH, _Key, _Body, _Extras, _CAS) ->
    erlcache_cache:flush(),
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
handle_command(?DELETE, Key, _Body, _Extras, _CAS) ->
    erlcache_cache:delete(Key),
    {reply, #response{status=?SUCCESS}};
handle_command(?SET, Key, Value, Extras, CAS) ->
    <<Flags:32, Expiration:32>> = Extras,
    case erlcache_cache:set(Key, Value, Expiration, Flags, CAS) of
	ok ->
	    {reply, #response{status=?SUCCESS}};
	not_found ->
	    {reply, #response{status=?NOT_FOUND}};
	key_exists ->
	    {reply, #response{status=?KEY_EXISTS}}
    end;
handle_command(?ADD, Key, Value, Extras, CAS) ->
    <<Flags:32, Expiration:32>> = Extras,
    Success = erlcache_cache:add(Key, Value, Expiration, Flags, CAS),
    Status = case Success of
		 ok -> ?SUCCESS;
		 key_exists -> ?KEY_EXISTS
	     end,
    {reply, #response{status=Status}};
handle_command(?REPLACE, Key, Value, Extras, CAS) ->
    <<Flags:32, Expiration:32>> = Extras,
    Success = erlcache_cache:replace(Key, Value, Expiration, Flags, CAS),
    Status = case Success of
		 ok -> ?SUCCESS;
		 not_found -> ?NOT_FOUND
	     end,
    {reply, #response{status=Status}};
handle_command(?VERSION, <<>>, <<>>, <<>>, _CAS) ->
    {reply, #response{status=?SUCCESS, body= <<"1.2.3">>}};
handle_command(?INCR, Key, <<>>, <<Amount:64, Initial:64, Expiration:32>>, _CAS) ->
    case erlcache_cache:incr(Key, Amount, Initial, Expiration) of
	{ok, Value} ->
	    {reply, #response{status=?SUCCESS, body= <<Value:64>>}};
	not_found ->
	    {reply, #response{status=?NOT_FOUND}}
    end;
handle_command(?DECR, Key, <<>>, <<Amount:64, Initial:64, Expiration:32>>, _CAS) ->
    case erlcache_cache:incr(Key, -Amount, Initial, Expiration) of
	{ok, Value} ->
	    {reply, #response{status=?SUCCESS, body= <<Value:64>>}};
	not_found ->
	    {reply, #response{status=?NOT_FOUND}}
    end;
handle_command(_, _, _, _, _) ->
    io:format("No match", []),
    {reply, #response{status=?UNKNOWN_COMMAND}}.


response_to_binary(Response, Opaque, Opcode) ->
    #response{status=Status,
	      body=Body,
	      extras=Extras,
	      key=Key,
	      cas=CAS
	     } = Response,
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

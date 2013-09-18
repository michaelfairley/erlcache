-module(erlcache_protocol).
-include("erlcache_constants.hrl").

-export([start_link/4]).
-export([init/4]).

-record(response, {
	  opcode,
	  status,
	  body = <<>>,
	  key = <<>>,
	  extras = <<>>
	 }).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
        {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport).

recv(Transport, Socket, Length) ->
    if
	Length > 0 ->
	    {ok, Data} = Transport:recv(Socket, Length, 1000),
	    Data;
	true ->
	    <<>>
    end.

loop(Socket, Transport) ->
    case Transport:recv(Socket, ?HEADER_LENGTH, 1000) of
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

	    Response = handle_command(Opcode, Key, Body, Extras, CAS),
	    ResponseData = response_to_binary(Response, Opaque),

            Transport:send(Socket, ResponseData),
	    loop(Socket, Transport);
	_ ->
	    ok = Transport:close(Socket)
    end.

handle_command(?NOOP, _Key, _Body, _Extras, _CAS) ->
    #response{opcode=?NOOP, status=?SUCCESS};
handle_command(?FLUSH, _Key, _Body, _Extras, _CAS) ->
    erlcache_cache:flush(),
    #response{opcode=?FLUSH, status=?SUCCESS};
handle_command(?GET, Key, _Body, _Extras, _CAS) ->
    case erlcache_cache:get(Key) of
	{ok, Value, Flags} ->
	    #response{opcode=?GET, status=?SUCCESS, body=Value, extras=Flags};
	notfound ->
	    #response{opcode=?GET, status=?NOT_FOUND}
    end;
handle_command(?SET, Key, Value, Extras, _CAS) ->
    <<Flags:32, Expiration:32>> = Extras,
    ok = erlcache_cache:set(Key, Value, Expiration, Flags),
    #response{opcode=?SET, status=?SUCCESS}.

response_to_binary(Response, Opaque) ->
    #response{opcode=Opcode,
	      status=Status,
	      body=Body,
	      extras=Extras,
	      key=Key
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
      0:64>>,
     Extras,
     Key,
     Body].

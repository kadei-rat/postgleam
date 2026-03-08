-module(postgleam_ffi).
-export([crypto_exor/2, crypto_strong_rand_bytes/1, crypto_mac_hmac/2, md5_hash/1, sha256_hash/1,
         encode_float32/1, decode_float32/1, encode_numeric/1,
         int_shr/2, int_shl/2, int_band/2, int_bor/2, int_to_hex/1, hex_to_int/1,
         ssl_upgrade/4, ssl_send/2, ssl_recv/2, ssl_close/1,
         monotonic_time_ms/0]).

crypto_exor(A, B) ->
    crypto:exor(A, B).

crypto_strong_rand_bytes(N) ->
    crypto:strong_rand_bytes(N).

crypto_mac_hmac(Key, Data) ->
    crypto:mac(hmac, sha256, Key, Data).

md5_hash(Data) ->
    crypto:hash(md5, Data).

sha256_hash(Data) ->
    crypto:hash(sha256, Data).

encode_float32(F) ->
    <<F:32/float-big>>.

decode_float32(<<F:32/float-big>>) ->
    {ok, F};
decode_float32(_) ->
    {error, <<"float4 codec: expected 4 bytes">>}.

%% Encode a numeric string to PostgreSQL binary format
encode_numeric(S) when is_binary(S) ->
    case S of
        <<"NaN">> -> <<0:16, 0:16, 16#C000:16, 0:16>>;
        <<"Infinity">> -> <<0:16, 0:16, 16#D000:16, 0:16>>;
        <<"-Infinity">> -> <<0:16, 0:16, 16#F000:16, 0:16>>;
        _ -> encode_numeric_value(S)
    end.

encode_numeric_value(S) ->
    %% Parse sign
    {Sign, Rest} = case S of
        <<"-", R/binary>> -> {16#4000, R};
        _ -> {0, S}
    end,
    %% Split on decimal point
    {IntPart, FracPart, Scale} = case binary:split(Rest, <<".">>) of
        [I, F] -> {I, F, byte_size(F)};
        [I] -> {I, <<>>, 0}
    end,
    %% Convert to list of base-10000 digits
    IntDigits = int_to_base10000(IntPart),
    FracDigits = frac_to_base10000(FracPart),
    AllDigits = IntDigits ++ FracDigits,
    %% Remove trailing zeros from digit list
    AllDigitsTrimmed = trim_trailing_zeros(AllDigits),
    NDigits = length(AllDigitsTrimmed),
    Weight = length(IntDigits) - 1,
    DigitsBin = << <<D:16>> || D <- AllDigitsTrimmed >>,
    <<NDigits:16, Weight:16/signed, Sign:16, Scale:16, DigitsBin/binary>>.

int_to_base10000(<<>>) -> [0];
int_to_base10000(<<"0">>) -> [0];
int_to_base10000(Bin) ->
    S = binary_to_list(Bin),
    Len = length(S),
    %% Pad to multiple of 4 on the left
    PadLen = (4 - (Len rem 4)) rem 4,
    Padded = lists:duplicate(PadLen, $0) ++ S,
    groups_of_4(Padded).

frac_to_base10000(<<>>) -> [];
frac_to_base10000(Bin) ->
    S = binary_to_list(Bin),
    Len = length(S),
    %% Pad to multiple of 4 on the right
    PadLen = (4 - (Len rem 4)) rem 4,
    Padded = S ++ lists:duplicate(PadLen, $0),
    groups_of_4(Padded).

groups_of_4([]) -> [];
groups_of_4([A,B,C,D|Rest]) ->
    N = (A - $0) * 1000 + (B - $0) * 100 + (C - $0) * 10 + (D - $0),
    [N | groups_of_4(Rest)].

trim_trailing_zeros([]) -> [];
trim_trailing_zeros(L) ->
    lists:reverse(drop_while_zero(lists:reverse(L))).

drop_while_zero([0|T]) -> drop_while_zero(T);
drop_while_zero(L) -> L.

%% Bitwise operations for replication LSN handling
int_shr(A, B) -> A bsr B.
int_shl(A, B) -> A bsl B.
int_band(A, B) -> A band B.
int_bor(A, B) -> A bor B.

int_to_hex(N) ->
    list_to_binary(integer_to_list(N, 16)).

hex_to_int(S) ->
    try
        {ok, list_to_integer(binary_to_list(S), 16)}
    catch
        _:_ -> {error, nil}
    end.

%% SSL/TLS support — mirrors Postgrex's {mod, sock} pattern

%% Upgrade a TCP socket to SSL, matching Postgrex's ssl_connect pattern.
%% In mug 3.x, Socket is an external type — it IS the raw gen_tcp port directly.
%% Verify=true: verify_peer with system CA certs + SNI
%% Verify=false: verify_none (for self-signed / Neon)
ssl_upgrade(MugSocket, Host, Timeout, Verify) ->
    TcpSock = MugSocket,
    HostCharlist = binary_to_list(Host),
    SslOpts = case Verify of
        true ->
            [{verify, verify_peer},
             {cacerts, public_key:cacerts_get()},
             {server_name_indication, HostCharlist},
             {customize_hostname_check,
              [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}];
        false ->
            [{verify, verify_none},
             {server_name_indication, HostCharlist}]
    end,
    case ssl:connect(TcpSock, SslOpts, Timeout) of
        {ok, SslSock} ->
            {ok, SslSock};
        {error, Reason} ->
            {error, list_to_binary(io_lib:format("~p", [Reason]))}
    end.

ssl_send(SslSock, Data) ->
    case ssl:send(SslSock, Data) of
        ok -> {ok, nil};
        {error, Reason} ->
            {error, list_to_binary(io_lib:format("~p", [Reason]))}
    end.

ssl_recv(SslSock, Timeout) ->
    case ssl:recv(SslSock, 0, Timeout) of
        {ok, Data} ->
            {ok, Data};
        {error, Reason} ->
            {error, list_to_binary(io_lib:format("~p", [Reason]))}
    end.

ssl_close(SslSock) ->
    ssl:close(SslSock),
    nil.

%% Monotonic time in milliseconds (for pool queue timeout tracking)
monotonic_time_ms() ->
    erlang:monotonic_time(millisecond).


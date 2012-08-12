-module(simple_cache).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {table, heap}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, set/2, set/3, lookup/1, lookup/2, delete/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

set(Key, Value) ->
    gen_server:cast({set, Key, Value, infinity}).

set(Key, _Value, 0) -> delete(Key);

set(Key, Value, Expires) when is_number(Expires), Expires > 0 ->
    gen_server:cast(?SERVER, {set, Key, Value, Expires}).

lookup(Key) -> get(?SERVER, Key).

lookup(Key, Default) ->
    case lookup(Key) of
        {ok, Value} ->
            Value;
        {error, missing} ->
            Default
    end.
    
delete(Key) ->
    gen_server:cast(?SERVER, {delete, Key}),
    ok.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    State = #state{table=ets:new(?SERVER, [{read_concurrency, true}, named_table]), heap=pairheap:new()},
    {ok, State}.

% Try to get the key.
handle_call(_Request, _From, State) ->
    {NewHeap, Timeout} = clean_expired(State),
    {reply, ok, State#state{heap=NewHeap}, Timeout}.

% Sets a key without an expiration.
handle_cast({set, Key, Value, infinity}, #state{table=Table, heap=Heap} = State) ->
    ets:insert(Table, {Key, Value, infinity}),
    {NewHeap, Timeout} = clean_expired(Table, Heap),
    {noreply, State#state{heap=NewHeap}, Timeout};

% Sets a key with an expiration.
handle_cast({set, Key, Value, Expires}, #state{table=Table, heap=Heap} = State) ->
    ExpireTime = current_time() + Expires,
    ets:insert(Table, {Key, Value, ExpireTime}),
    {NewHeap, Timeout} = clean_expired(Table, pairheap:insert(Heap, {Key, ExpireTime})),
    {noreply, State#state{heap=NewHeap}, Timeout};

% Delete a key from the cache.
handle_cast({delete, Key}, #state{table=Table}=State) ->
    ets:delete(Table, Key),
    {NewHeap, Timeout} = clean_expired(State),
    {noreply, State#state{heap=NewHeap}, Timeout};

handle_cast(_Msg, State) ->
    {NewHeap, Timeout} = clean_expired(State),
    {noreply, State#state{heap=NewHeap}, Timeout}.

% We've timed out, so a key has in all likelihood expired.
handle_info(timeout, State) ->
    {NewHeap, Timeout} = clean_expired(State),
    {noreply, State#state{heap=NewHeap}, Timeout};

handle_info(_Info, State) ->
    {NewHeap, Timeout} = clean_expired(State),
    {noreply, State#state{heap=NewHeap}, Timeout}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
current_time() ->
    calendar:datetime_to_gregorian_seconds(erlang:localtime()).

clean_expired(#state{table=T, heap=H}) ->
    clean_expired(T, H).
clean_expired(Table, Heap) ->
    clean_expired(Table, Heap, current_time()).

% Deletes expired keys
clean_expired(Table, Heap, CurrentTime) ->
    case pairheap:find_min(Heap) of
        % When we have a key that looks expired.
        {ok, {Key, Expires}} when Expires =< CurrentTime ->

            % Grab the key
            case ets:lookup(Table, Key) of
                % We need to check that the Expiration date hasn't been updated 
                % to a later time.
                [{Key, _Value, RealExpires}] when RealExpires =< CurrentTime ->
                    ets:delete(Table, Key);

                % Key is no longer in the table or hasn't expired, move along.
                _Other -> ok
            end,
            {ok, NewHeap} = pairheap:delete_min(Heap),
            clean_expired(Table, NewHeap);

        % Non-expired key
        {ok, {_Key, Expires}} ->
            {Heap, Expires - CurrentTime};

        % Empty Heap, never times out.
        {error, empty} ->
            {Heap, infinity}
    end.

% Retrieves an item from the cache.
get(Table, Key) ->
     case ets:lookup(Table, Key) of
        [{Key, Value, _Expires}] ->
            {ok, Value};
        [] ->
            {error, missing}
    end.


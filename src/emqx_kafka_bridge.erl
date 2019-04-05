%%--------------------------------------------------------------------
%% Copyright (c) 2015-2017 Feng Lee <feng@emqtt.io>.
%%
%% Modified by Ramez Hanna <rhanna@iotblue.net>
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_kafka_bridge).

% -include("emqx_kafka_bridge.hrl").

-include_lib("emqx/include/emqx.hrl").

-record(state, {payload_topic, event_topic}).


% -import(string,[concat/2]).
% -import(lists,[nth/2]). 

-export([load/1, unload/0]).

%% Hooks functions
-export([on_client_connected/4, on_client_disconnected/3]).
-export([on_client_subscribe/3, on_client_unsubscribe/3]).
-export([on_session_created/3, on_session_resumed/3, on_session_terminated/3]).
-export([on_session_subscribed/4, on_session_unsubscribed/4]).
-export([on_message_publish/2, on_message_delivered/3, on_message_acked/3, on_message_dropped/3]).


%% Called when the plugin application start
load(Env) ->
	ekaf_init([Env]),
    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/3, [Env]),
    emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3, [Env]),
    emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    emqx:hook('session.resumed', fun ?MODULE:on_session_resumed/3, [Env]),
    emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/3, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
    emqx:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]),
    emqx:hook('message.dropped', fun ?MODULE:on_message_dropped/3, [Env]).

ekaf_init(_Env) ->
    {ok, BrokerValues} = application:get_env(emqx_kafka_bridge, broker),
    KafkaHost = proplists:get_value(host, BrokerValues),
    KafkaPort = proplists:get_value(port, BrokerValues),
    KafkaPartitionStrategy= proplists:get_value(partitionstrategy, BrokerValues),
    KafkaPartitionWorkers= proplists:get_value(partitionworkers, BrokerValues),
    KafkaPayloadTopic = proplists:get_value(payloadtopic, BrokerValues),
    KafkaEventTopic = proplists:get_value(eventtopic, BrokerValues),
    application:set_env(ekaf, ekaf_bootstrap_broker,  {KafkaHost, list_to_integer(KafkaPort)}),
    application:set_env(ekaf, ekaf_partition_strategy, KafkaPartitionStrategy),
    application:set_env(ekaf, ekaf_per_partition_workers, KafkaPartitionWorkers),
    application:set_env(ekaf, ekaf_per_partition_workers_max, 10),
    {ok, #state{payload_topic = KafkaPayloadTopic, event_topic = KafkaEventTopic}},
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(ekaf).

on_client_connected(#{client_id := ClientId}, ConnAck, ConnAttrs, _Env) ->
    io:format("Client(~s) connected, connack: ~w, conn_attrs:~p~n", [ClientId, ConnAck, ConnAttrs]),
    Event = [{action, <<"connected">>}, {clientid, ClientId}, {result, ConnAck}],
    produce_kafka_log(Event, _Env).

on_client_disconnected(#{client_id := ClientId}, ReasonCode, _Env) ->
    io:format("Client(~s) disconnected, reason_code: ~w~n", [ClientId, ReasonCode]),
    Event = [{action, <<"disconnected">>}, {clientid, ClientId}, {result, ReasonCode}],
    produce_kafka_log(Event, _Env).

on_client_subscribe(#{client_id := ClientId}, RawTopicFilters, _Env) ->
    io:format("Client(~s) will subscribe: ~p~n", [ClientId, RawTopicFilters]),
    {ok, RawTopicFilters}.

on_client_unsubscribe(#{client_id := ClientId}, RawTopicFilters, _Env) ->
    io:format("Client(~s) unsubscribe ~p~n", [ClientId, RawTopicFilters]),
    {ok, RawTopicFilters}.

on_session_created(#{client_id := ClientId}, SessAttrs, _Env) ->
    io:format("Session(~s) created: ~p~n", [ClientId, SessAttrs]).

on_session_resumed(#{client_id := ClientId}, SessAttrs, _Env) ->
    io:format("Session(~s) resumed: ~p~n", [ClientId, SessAttrs]).

on_session_subscribed(#{client_id := ClientId}, Topic, SubOpts, _Env) ->
    io:format("Session(~s) subscribe ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

on_session_unsubscribed(#{client_id := ClientId}, Topic, Opts, _Env) ->
    io:format("Session(~s) unsubscribe ~s with opts: ~p~n", [ClientId, Topic, Opts]).

on_session_terminated(#{client_id := ClientId}, ReasonCode, _Env) ->
    io:format("Session(~s) terminated: ~p.", [ClientId, ReasonCode]).

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    io:format("Publish ~s~n", [emqx_message:format(Message)]),
    {ok, Message}.

on_message_delivered(#{client_id := ClientId}, Message, _Env) ->
    io:format("Delivered message to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

on_message_acked(#{client_id := ClientId}, Message, _Env) ->
    io:format("Session(~s) acked message: ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

on_message_dropped(_By, #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    ok;
on_message_dropped(#{node := Node}, Message, _Env) ->
    io:format("Message dropped by node ~s: ~s~n", [Node, emqx_message:format(Message)]);
on_message_dropped(#{client_id := ClientId}, Message, _Env) ->
io:format("Message dropped by client ~s: ~s~n", [ClientId, emqx_message:format(Message)]).

% on_client_connected(ConnAck, Client = #mqtt_client{client_id = ClientId, username = Username}, _Env) ->
%     % io:format("client ~s/~s will connected: ~w.~n", [ClientId, Username, ConnAck]),
%     Event = [{action, <<"connected">>},
%                 {clientid, ClientId},
%                 {username, Username},
%                 {result, ConnAck}],
%     produce_kafka_log(Event),
%     {ok, Client}.

% on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId, username = Username}, _Env) ->
%     % io:format("client ~s/~s will connected: ~w~n", [ClientId, Username, Reason]),
%     Event = [{action, <<"disconnected">>},
%                 {clientid, ClientId},
%                 {username, Username},
%                 {result, Reason}],
%     produce_kafka_log(Event),
%     ok.


% % on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
% %     % io:format("client(~s/~s) will subscribe: ~p~n", [Username, ClientId, TopicTable]),
% %     {ok, TopicTable}.
    
% % on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
% %     % io:format("client(~s/~s) unsubscribe ~p~n", [ClientId, Username, TopicTable]),
% %     {ok, TopicTable}.

% % on_session_created(ClientId, Username, _Env) ->
% %     % io:format("session(~s/~s) created~n", [ClientId, Username]),
% %     Event = [{action, connected},
% %                 {clientid, ClientId},
% %                 {username, Username}],
% %     produce_kafka_log(Event).

% % on_session_subscribed(ClientId, Username, {Topic, Opts}, _Env) ->
% %     % io:format("session(~s/~s) subscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
% %     {ok, {Topic, Opts}}.

% % on_session_unsubscribed(ClientId, Username, {Topic, Opts}, _Env) ->
% %     % io:format("session(~s/~s) unsubscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
% %     ok.

% % on_session_terminated(ClientId, Username, Reason, _Env) ->
% %     % io:format("session(~s/~s/~s) terminated~n", [ClientId, Username, Reason]),
% %     Event = [{action, disconnected},
% %                 {clientid, ClientId},
% %                 {username, Username}],
% %     produce_kafka_log(Event).

% %% transform message and return
% on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
%     % io:format("message publish: ~p.", [topic]),
%     {ok, Message};

% on_message_publish(Message, _Env) ->
%     {ok, Payload} = format_payload(Message),
%     produce_kafka_payload(Payload),	
%     {ok, Message}.

% % on_message_delivered(ClientId, Username, Message, _Env) ->
% %     % io:format("delivered to client(~s/~s): ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
% %     Event = [{action, <<"delivered">>},
% %                 {clientid, ClientId},
% %                 {username, Username},
% %                 {message, emqttd_message:format(Message)}],
% %     produce_kafka_log(Event),
% %     {ok, Message}.

% % on_message_acked(ClientId, Username, Message, _Env) ->
% %     % io:format("client(~s/~s) acked: ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
% %     {ok, Message}.

% format_event(Action, Client) ->
%     Event = [{action, Action},
%                 {clientid, Client#mqtt_client.client_id},
%                 {username, Client#mqtt_client.username}],
%     {ok, Event}.

format_payload(Message) ->
    {ClientId, Username} = format_from(Message#mqtt_message.from),
    Payload = [{action, <<"message_publish">>},
                  {clientid, ClientId},
                  {username, Username},
                  {topic, Message#mqtt_message.topic},
                  {payload, Message#mqtt_message.payload},
                  {ts, emqttd_time:now_secs(Message#mqtt_message.timestamp)}],
    {ok, Payload}.

format_from({ClientId, Username}) ->
    {ClientId, Username};
format_from(From) when is_atom(From) ->
    {a2b(From), a2b(From)};
format_from(_) ->
    {<<>>, <<>>}.

a2b(A) -> erlang:atom_to_binary(A, utf8).

produce_kafka_payload(Message, #state{payload_topic=PayloadTopic}) ->
    Payload = jsx:encode(Message),
    ok = ekaf:produce_async(PayloadTopic, Payload),
    ok.
    

produce_kafka_log(Message, #state{event_topic=EventTopic}) ->
    Payload = jsx:encode(Message),
    ok = ekaf:produce_async(EventTopic, Payload),
    ok.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/3),
    emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3),
    emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqx:unhook('session.resumed', fun ?MODULE:on_session_resumed/3),
    emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/3),
    emqx:unhook('message.acked', fun ?MODULE:on_message_acked/3),
    emqx:unhook('message.dropped', fun ?MODULE:on_message_dropped/3).
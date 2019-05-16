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

% -import(string,[concat/2]).
% -import(lists,[nth/2]). 

-export([load/1, unload/0]).

%% Hooks functions
-export([on_client_connected/4, on_client_disconnected/3]).
-export([on_message_publish/2, on_message_delivered/3, on_message_acked/3]).
% -export([on_client_subscribe/3, on_client_unsubscribe/3]).
% -export([on_session_created/3, on_session_resumed/3, on_session_terminated/3]).
% -export([on_session_subscribed/4, on_session_unsubscribed/4]).
% -export([on_message_publish/2, on_message_delivered/3, on_message_acked/3, on_message_dropped/3]).


%% Called when the plugin application start
load(Env) ->
	ekaf_init([Env]),
    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
    emqx:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]).
    % emqx:hook('message.dropped', fun ?MODULE:on_message_dropped/3, [Env]).
    % emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/3, [Env]),
    % emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3, [Env]),
    % emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    % emqx:hook('session.resumed', fun ?MODULE:on_session_resumed/3, [Env]),
    % emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    % emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    % emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/3, [Env]),
    

ekaf_init(_Env) ->
    {ok, BrokerValues} = application:get_env(emqx_kafka_bridge, broker),
    KafkaHost = proplists:get_value(host, BrokerValues),
    KafkaPort = proplists:get_value(port, BrokerValues),
    KafkaPartitionStrategy= proplists:get_value(partitionstrategy, BrokerValues),
    KafkaPartitionWorkers= proplists:get_value(partitionworkers, BrokerValues),
    KafkaPublishTopic = proplists:get_value(publishtopic, BrokerValues),
    KafkaDeliveredTopic = proplists:get_value(deliveredtopic, BrokerValues),
    KafkaAckedTopic = proplists:get_value(ackedtopic, BrokerValues),
    KafkaClientConnectTopic = proplists:get_value(clientconnecttopic, BrokerValues),
    KafkaClientDisconnectTopic = proplists:get_value(clientdisconnecttopic, BrokerValues),
    application:set_env(ekaf, ekaf_bootstrap_broker,  {KafkaHost, list_to_integer(KafkaPort)}),
    application:set_env(ekaf, ekaf_partition_strategy, KafkaPartitionStrategy),
    application:set_env(ekaf, ekaf_per_partition_workers, KafkaPartitionWorkers),
    application:set_env(ekaf, ekaf_per_partition_workers_max, 10),
    application:set_env(emqx_kafka_bridge, kafka_publish_topic, KafkaPublishTopic),
    application:set_env(emqx_kafka_bridge, kafka_delivered_topic, KafkaDeliveredTopic),
    application:set_env(emqx_kafka_bridge, kafka_acked_topic, KafkaAckedTopic),
    application:set_env(emqx_kafka_bridge, kafka_client_connect_topic, KafkaClientConnectTopic),
    application:set_env(emqx_kafka_bridge, kafka_client_disconnect_topic, KafkaClientDisconnectTopic),
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(ekaf).

on_client_connected(#{client_id := ClientId}, ConnAck, ConnAttrs, _Env) ->
    % io:format("Client(~s) connected, connack: ~w, conn_attrs:~p~n", [ClientId, ConnAck, ConnAttrs]),
    Message = [{clientid, ClientId}, {result, ConnAck}],
    if
        ConnAck == 0 ->  produce_kafka_client_connected(Message, _Env)
    end.

on_client_disconnected(#{client_id := ClientId}, ReasonCode, _Env) ->
    % io:format("Client(~s) disconnected, reason_code: ~w~n", [ClientId, ReasonCode]),
    Message = [{clientid, ClientId}, {result, ReasonCode}],
    produce_kafka_client_disconnected(Message, _Env).

% on_client_subscribe(#{client_id := ClientId}, RawTopicFilters, _Env) ->
%     io:format("Client(~s) will subscribe: ~p~n", [ClientId, RawTopicFilters]),
%     {ok, RawTopicFilters}.

% on_client_unsubscribe(#{client_id := ClientId}, RawTopicFilters, _Env) ->
%     io:format("Client(~s) unsubscribe ~p~n", [ClientId, RawTopicFilters]),
%     {ok, RawTopicFilters}.

% on_session_created(#{client_id := ClientId}, SessAttrs, _Env) ->
%     io:format("Session(~s) created: ~p~n", [ClientId, SessAttrs]).

% on_session_resumed(#{client_id := ClientId}, SessAttrs, _Env) ->
%     io:format("Session(~s) resumed: ~p~n", [ClientId, SessAttrs]).

% on_session_subscribed(#{client_id := ClientId}, Topic, SubOpts, _Env) ->
%     io:format("Session(~s) subscribe ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

% on_session_unsubscribed(#{client_id := ClientId}, Topic, Opts, _Env) ->
%     io:format("Session(~s) unsubscribe ~s with opts: ~p~n", [ClientId, Topic, Opts]).

% on_session_terminated(#{client_id := ClientId}, ReasonCode, _Env) ->
%     io:format("Session(~s) terminated: ~p.", [ClientId, ReasonCode]).

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    % io:format("Publish ~s~n", [emqx_message:format(Message)]),
    % {ok, Payload} = format_payload(Message),
    Payload = [{clientid, Message#message.from}, 
        {topic, Message#message.topic},
        {payload, Message#message.payload},
        {time, emqx_time:now_secs(Message#message.timestamp)}],
    produce_kafka_message_publish(Payload, _Env),	
    {ok, Message}.

on_message_delivered(#{client_id := ClientId}, Message, _Env) ->
    io:format("Delivered message to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
    Event = [{clientid, ClientId},
        {message, emqx_message:format(Message)}],
        produce_kafka_message_delivered(Event, _Env),
    {ok, Message}.

on_message_acked(#{client_id := ClientId}, Message, _Env) ->
    io:format("Session(~s) acked message: ~s~n", [ClientId, emqx_message:format(Message)]),
    Event = [{clientid, ClientId},
        {message, emqx_message:format(Message)}],
        produce_kafka_message_acked(Event, _Env),
    {ok, Message}.

% on_message_dropped(_By, #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
%     ok;
% on_message_dropped(#{node := Node}, Message, _Env) ->
%     io:format("Message dropped by node ~s: ~s~n", [Node, emqx_message:format(Message)]);
% on_message_dropped(#{client_id := ClientId}, Message, _Env) ->
%     io:format("Message dropped by client ~s: ~s~n", [ClientId, emqx_message:format(Message)]).   

produce_kafka_message_publish(Message, _Env) ->
    {ok, Topic} = application:get_env(emqx_kafka_bridge, kafka_publish_topic),
    Payload = jsx:encode(Message),
    ok = ekaf:produce_async(list_to_binary(Topic), Payload),
    ok.

produce_kafka_message_delivered(Message, _Env) ->
    {ok, Topic} = application:get_env(emqx_kafka_bridge, kafka_delivered_topic),
    Payload = jsx:encode(Message),
    ok = ekaf:produce_async(list_to_binary(Topic), Payload),
    ok.

produce_kafka_message_acked(Message, _Env) ->
    {ok, Topic} = application:get_env(emqx_kafka_bridge, kafka_acked_topic),
    Payload = jsx:encode(Message),
    ok = ekaf:produce_async(list_to_binary(Topic), Payload),
    ok.

produce_kafka_client_connected(Message, _Env) ->
    {ok, Topic} = application:get_env(emqx_kafka_bridge, kafka_client_connect_topic),
    Payload = jsx:encode(Message),
    ok = ekaf:produce_async(list_to_binary(Topic), Payload),
    ok.

produce_kafka_client_disconnected(Message, _Env) ->
    {ok, Topic} = application:get_env(emqx_kafka_bridge, kafka_client_disconnect_topic),
    Payload = jsx:encode(Message),
    ok = ekaf:produce_async(list_to_binary(Topic), Payload),
    ok.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/3),
    emqx:unhook('message.acked', fun ?MODULE:on_message_acked/3).
    % emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/3),
    % emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3),
    % emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
    % emqx:unhook('session.resumed', fun ?MODULE:on_session_resumed/3),
    % emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    % emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    % emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/3),
    % emqx:unhook('message.dropped', fun ?MODULE:on_message_dropped/3).
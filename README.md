
# EMQX kafka bridge

This is a plugin for the EMQX broker that sends all messages received by the broker to kafka.

## Build the EMQ broker

1. Clone emqx-rel project

   We need to clone the EMQ-x project [GITHUB](https://github.com/emqx/emqx-rel)

```shell
  git clone https://github.com/emqx/emqx-rel
```
Documentaion is not complete yet.

Start the EMQ broker and load the plugin 
-----------------
1) cd emq-relx/_rel/emqttd
2) ./bin/emqttd start
3) ./bin/emqttd_ctl plugins load emqttd_kafka_bridge

Test
-----------------
Send a MQTT message on a random topic from a MQTT client to your EMQ broker.

The following should be received by your kafka consumer :

  {"topic":"yourtopic", "message":[yourmessage]}
This is the format in which kafka will receive the MQTT messages

If Kafka consumer shows no messages even after publishing to EMQTT - ACL makes the plugin fail, so please remove all the ACL related code to ensure it runs properly. We will soon push the updated (Working) code to the repository. 

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details


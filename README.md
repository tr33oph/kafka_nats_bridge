# kafka_nats_bridge
Get message from kafka and route to nats by some field value.

Used when you need filter message, but kafka does not support server side filter. NATS supports 100M subs/topics, and support a.* ,  so this a ptoxy layer for filter messages.

## swim-qpid-proton v0.1.0

### Introduction
`swim-qpid-proton` is an extension of [python-qpid_proton](https://pypi.org/project/python-qpid-proton/). It enables
the user to register callbacks that will be called upon sending and reception of a message.

### Terminology

##### message_producer
A `message_producer` is a callback defined by the end user i.e. a publisher service. It accepts optionally a `context`
parameter.

##### message_consumer
A `message_consumer` is a callback defined by the end user i.e. a subscriber service. It accepts a`proton.Message` 
parameter.

##### Producer
`Producer` is an extension of the `proton.MessagingHandler` that keeps a list of message producers identified by a unique id (topic name). A message producer can be invoked via 
the `Producer` on demand or it can be scheduled to be executed in interval periods. All produced messages will be sent
in the broker via a `proton.Sender` instance routed to a dedicated topic based on the id of the message producer.

##### Consumer
`Consumer` is an extension of the `proton.MessagingHandler` that keeps a list of message consumers (callbacks defined
by the end user i.e. a subscriber service) identified by the broker queue they are expecting a message from. The message
consumer is invoked upon message reception from its queue and consumes the incoming message accordingly.

##### PubSubContainer
`PubSubContainer` behaves like a `proton.Container` accepting a `proton.MessagingHandler` but it also provides the 
possibility of running in threaded mode. This allows a messaging handler such as `Producer` or `Consumer` to be used
freely after the containers initialization and register message producers or consumers

##### ProducerContainer
`ProducerContainer` is a container that uses the Producer messaging handler out of the box

##### ConsumerContainer
`ConsumerContainer` is a container that uses the Consumer messaging handler out of the box

### Configuration
A container is created by providing a config object (dict) with the following properties:
```shell
{
    "host": "rabbitmq:5671",
    "cert_file": "path/to/client_certificate.pem",  # to be used for TLS connections 
    "cert_key": "path/to/client_key.pem"            # to be used for TLS connections
    "sasl_user: "username"                          # to be used for SASL connections
    "sasl_password": "password"                     # to be used for SASL connections  
    "cert_db": "path/to/ca_certificate.pem",        # to be used for both TLS and SASL connections
}
```
> In case a secured connection is not required then the `host` parameter should be enough.

### Examples

##### Producer
```python

from swim_proton.containers import ProducerContainer

config = {}  # connection settings here 

container = ProducerContainer.create_from_config(config)

container.producer.add_message_producer('topic1', lambda context: 'topic1 message' + context, interval_in_sec=5)
container.producer.add_message_producer('topic2', lambda context: 'topic2 message' + context)

# both of the message producers can be invoked on demand. However, the message producer 'topic1' will also be invoked 
# every 5 seconds
container.producer.trigger_message('topic1', context='extra message')
container.producer.trigger_message('topic2', context='extra message')
```

##### Consumer

```python

from swim_proton.containers import ConsumerContainer

config = {}  # connection settings here 

container = ConsumerContainer.create_from_config(config)

# the message consumer will be associated with the provided queue and be invoked every time a new message arrives.
container.consumer.attach_message_consumer('queue_name', lambda context: context)
```

> The communication of the queue name between the producer and the consumer is out of the scope of this library. 
> Typically a coordination system is required such as [SubscriptionManager](https://github.com/eurocontrol-swim/subscription-manager).

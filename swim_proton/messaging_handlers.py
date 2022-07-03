
"""
Copyright 2019 EUROCONTROL
==========================================

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
   disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
   disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

==========================================

Editorial note: this license is an instance of the BSD license template as provided by the Open Source Initiative:
http://opensource.org/licenses/BSD-3-Clause

Details on EUROCONTROL: http://www.eurocontrol.int
"""

import logging
import traceback
from dataclasses import dataclass
from typing import Optional, List, Any, Dict, Callable, Tuple, Union

import proton
from proton.handlers import MessagingHandler

__author__ = "EUROCONTROL (SWIM)"

from swim_proton import ConfigDict
from swim_proton.connectors import Connector, TLSConnector, SASLConnector

_logger = logging.getLogger(__name__)


class ScheduledTask(MessagingHandler):

    def __init__(self, task: Callable, interval_in_sec: int):
        """
        It inherits from `proton.MessagingHandler` in order to take advantage of its event
        scheduling functionality

        :param task:
        :param interval_in_sec:
        """
        MessagingHandler.__init__(self)

        self.task = task
        self.interval_in_sec = interval_in_sec

    def on_timer_task(self, event: proton.Event):
        """
        Is triggered upon a scheduled action. The first scheduling will be done by the broker
        handler and then the topic will be re-scheduling itself

        :param event:
        """
        # run the task
        self.task()

        # and re-schedule the task
        event.container.schedule(self.interval_in_sec, self)


class PubSubMessagingHandler(MessagingHandler):

    def __init__(self, connector: Connector) -> None:
        """
        Base class acting a MessagingHandler to a `proton.Container`. Any custom handler should
        inherit from this class.

        :param connector: takes care of the connection .i.e TSL, SASL etc
        """
        MessagingHandler.__init__(self)

        self.connector = connector

        self.container = None
        self.connection = None
        self.config = None

    def is_connected(self):
        return self.container is not None and self.connection is not None

    def connect(self, container):
        _logger.info(f'Connecting to {self.connector.url}')
        self.container = container
        try:
            self.connection = self.connector.connect(self.container)
        except Exception as e:
            _logger.error(f"Failed to connect to {self.connector.url}: {str(e)}")

    def on_start(self, event: proton.Event):
        """
        Is triggered upon running the `proton.Container` that uses this handler. It creates a
        connection to the broker and can be overridden for further startup functionality.

        :param event:
        """
        self.connect(event.container)

    @classmethod
    def create(cls,
               host: Optional[str] = None,
               cert_db: Optional[str] = None,
               cert_file: Optional[str] = None,
               cert_key: Optional[str] = None,
               cert_password: Optional[str] = None,
               sasl_user: Optional[str] = None,
               sasl_password: Optional[str] = None,
               allowed_mechs: Optional[str] = 'PLAIN',
               **kwargs):
        """

        :param host:
        :param cert_db:
        :param cert_file:
        :param cert_key:
        :param cert_password:
        :param sasl_user:
        :param sasl_password:
        :param allowed_mechs:
        :return:
        """
        if cert_file and cert_key:
            connector = TLSConnector(
                host=host,
                cert_file=cert_file,
                cert_key=cert_key,
                cert_db=cert_db,
                cert_password=cert_password
            )
        elif cert_db and sasl_user and sasl_password:
            connector = SASLConnector(
                host=host,
                user=sasl_user,
                password=sasl_password,
                cert_db=cert_db,
                allowed_mechs=allowed_mechs
            )
        else:
            connector = Connector(host=host)

        return cls(connector=connector)

    @classmethod
    def create_from_config(cls, config: ConfigDict):
        """
        Factory method for creating an instance from config values

        :param config:
        :return: BrokerHandler
        """

        return cls.create(**config)


@dataclass
class Messenger:
    id: Union[str, bytes]
    message_producer: Callable[..., proton.Message]
    interval_in_sec: Optional[int] = None
    after_send: Optional[List[Callable]] = None

    def __post_init__(self):
        if self.after_send is None:
            self.after_send = []

    def get_message(self, context: Optional[Any]) -> Optional[proton.Message]:
        try:
            message = self.message_producer(context=context)

            # the subject under which the message will be routed in the broker
            # typically it is the messenger_id or the topic name more generally
            if message:
                message.subject = self.id
        except Exception as e:
            message = None
            _logger.error(f"Error while producing message for messenger {self.id}: {str(e)}")

        return message


class Producer(PubSubMessagingHandler):

    def __init__(self, connector: Connector) -> None:
        """
        An implementation of a broker handler that is supposed to act as a publisher. It keeps a
        list of message producers which will generate respective messages which will be routed in
        the broker via a `proton.Sender` instance

        :param connector: takes care of the connection .i.e TSL, SASL etc
        """
        PubSubMessagingHandler.__init__(self, connector)

        self.endpoint: str = '/exchange/amq.topic'
        self._sender: Optional[proton.Sender] = None
        self._to_schedule: list[Messenger] = []

    def _create_sender_link(self, endpoint: str) -> Optional[proton.Sender]:
        try:
            self.container.create_sender(self.connection, endpoint)
            print("----- self.connection ",self.connection)
            print("----- endpoint ",  endpoint)

            _logger.debug(f"Created sender on endpoint {self.endpoint}")
        except Exception as e:
            _logger.error(f'Failed to create sender on endpoint {self.endpoint}: {str(e)}')
            sender = None

        return sender

    def on_start(self, event: proton.Event) -> None:
        """
        Is triggered upon running the `proton.Container` that uses this handler.
        If it has ScheduledMessageProducers items in its list they will be initialized and
        scheduled accordingly.

        :param event:
        """
        # call the parent event handler first to take care of the connection with the broker
        super().on_start(event)

        if not self.connection:
            return
        print("----- self.connection ",self.connection)
        print("----- event ",  event)
        print("----- self ",  self)
        self._sender = self._create_sender_link(self.endpoint)

        if not self._sender:
            return

        while self._to_schedule:
            messenger = self._to_schedule.pop(0)
            self._schedule_messenger(messenger)

    def schedule_messenger(self, messenger: Messenger) -> None:
        if not messenger.interval_in_sec:
            raise AssertionError(f"Invalid interval: {messenger.interval_in_sec}")

        if self.is_connected():
            self._schedule_messenger(messenger)
        else:
            self._to_schedule.append(messenger)

    def trigger_messenger(self, messenger: Messenger, context: Optional[Any] = None) -> None:
        """
        Generates a message via the messenger.message_producer and sends it in the broker.

        :param messenger:
        :param context:
        """

        _logger.info(f"Producing message for messenger {messenger.id}")
        message = messenger.get_message(context=context)

        if message:
            try:
                _logger.info(f"Attempting to send message for messenger `{messenger.id}`: {message}")
                self._send_message(message=message)
                _logger.info("Message sent")
            except Exception as e:
                traceback.print_exc()
                _logger.error(f"Error while sending message: {str(e)}")
            else:
                for callback in messenger.after_send:
                    try:
                        callback()
                    except Exception as e:
                        _logger.error(f"Error while running after send callback for messenger "
                                      "{messenger.id}")

    def _schedule_messenger(self, messenger: Messenger):
        task = ScheduledTask(
            task=lambda: self.trigger_messenger(messenger=messenger),
            interval_in_sec=messenger.interval_in_sec
        )
        self.container.schedule(task.interval_in_sec, task)

    def _send_message(self, message: proton.Message) -> None:
        """
        Sends the provided message via the broker.

        :param message:
        """

        if not self._sender:
            raise AssertionError("Sender has not been defined yet")

        if not self._sender.credit:
            raise ValueError("Not enough credit to send message")

        self._sender.send(message)

    @classmethod
    def create(cls,
               host: Optional[str] = None,
               cert_db: Optional[str] = None,
               cert_file: Optional[str] = None,
               cert_key: Optional[str] = None,
               cert_password: Optional[str] = None,
               sasl_user: Optional[str] = None,
               sasl_password: Optional[str] = None,
               allowed_mechs: Optional[str] = 'PLAIN',
               **kwargs):

        producer = super().create(host=host,
                                  cert_db=cert_db,
                                  cert_file=cert_file,
                                  cert_key=cert_key,
                                  cert_password=cert_password,
                                  sasl_user=sasl_user,
                                  sasl_password=sasl_password,
                                  allowed_mechs=allowed_mechs)

        endpoint = kwargs.pop('endpoint', None)
        if endpoint:
            producer.endpoint = endpoint

        return producer


class Consumer(PubSubMessagingHandler):

    def __init__(self, connector: Connector) -> None:
        """
        An implementation of a broker client that is supposed to act as subscriber.
        It subscribes to endpoints of the broker by creating instances of `proton.Receiver`
        for each one of them.

        :param connector: takes care of the connection .i.e TSL, SASL etc
        """
        PubSubMessagingHandler.__init__(self, connector)

        # keeps a dict of endpoints and their receiver/message_consumer
        self.endpoints_registry: Dict[str, Tuple[Optional[proton.Receiver], Callable]] = {}

    def _create_receiver_link(self, endpoint: str) -> Optional[proton.Receiver]:

        try:
            receiver = self.container.create_receiver(self.connection, endpoint)
            _logger.debug(f"Created receiver on endpoint {endpoint}")
        except Exception as e:
            receiver = None
            _logger.error(f"Failed to create receiver on {endpoint}: {str(e)}")

        return receiver

    def _get_endpoint_reg_by_receiver(self, receiver: proton.Receiver) -> Tuple[str, Callable]:
        """
        Find the endpoint and message_consumer that corresponds to the given receiver.
        :param receiver:
        :return:
        """
        for endpoint, (registered_receiver, message_consumer) in self.endpoints_registry.items():
            if receiver == registered_receiver:
                return endpoint, message_consumer

    def on_start(self, event: proton.Event) -> None:
        """
        Is triggered upon running the `proton.Container` that uses this handler.
        It checks if there are endpoints without a receiver attached to them and creates them

        :param event:
        """
        # call the parent event handler first to take care of the connection with the broker
        super().on_start(event)

        if not self.connection:
            return

        for endpoint, (receiver, message_consumer) in self.endpoints_registry.items():
            if receiver is None:
                receiver = self._create_receiver_link(endpoint)
                if receiver:
                    self.endpoints_registry[endpoint] = (receiver, message_consumer)

    def attach_message_consumer(self, endpoint: str, message_consumer: Callable) -> None:
        """
        Creates a new `proton.Receiver` and assigns the message consumer to it

        :param endpoint:
        :param message_consumer: consumes the messages coming from its assigned endpoint in the broker
        """
        self.endpoints_registry[endpoint] = (None, message_consumer)

        if self.is_connected():
            receiver = self._create_receiver_link(endpoint)
            if receiver:
                self.endpoints_registry[endpoint] = (receiver, message_consumer)

    def detach_message_consumer(self, endpoint: str) -> None:
        """
        Removes the receiver that corresponds to the given endpoint.

        :param endpoint:
        """
        receiver, _ = self.endpoints_registry.pop(endpoint)

        if receiver is not None:
            receiver.close()
            _logger.debug(f"Closed receiver {receiver} on endpoint {endpoint}")

    def on_message(self, event: proton.Event) -> None:
        """
        Is triggered upon reception of messages via the broker.

        :param event:
        """
        endpoint, message_consumer = self._get_endpoint_reg_by_receiver(event.receiver)

        try:
            message_consumer(event.message)
        except Exception as e:
            _logger.error(
                f"Error while processing message {event.message} on endpoint {endpoint}: {str(e)}")

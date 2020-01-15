
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
from functools import partial
from typing import Optional, List, Any, Dict, Callable, Tuple

import proton
from proton.handlers import MessagingHandler

__author__ = "EUROCONTROL (SWIM)"

from swim_proton import ConfigDict
from swim_proton.connectors import Connector, TLSConnector, SASLConnector
from swim_proton.utils import truncate_message

_logger = logging.getLogger(__name__)


class MessageProducerError(Exception):
    pass


class MessageConsumerError(Exception):
    pass


class TimerTask(MessagingHandler):

    def __init__(self, task: Callable, interval_in_sec: int):
        """
        It inherits from `proton.MessagingHandler` in order to take advantage of its event scheduling functionality

        :param task:
        :param interval_in_sec:
        """
        MessagingHandler.__init__(self)

        self.task = task
        self.interval_in_sec = interval_in_sec

    def on_timer_task(self, event: proton.Event):
        """
        Is triggered upon a scheduled action. The first scheduling will be done by the broker handler and then the topic
        will be re-scheduling itself

        :param event:
        """
        # run the task
        self.task()

        # and re-schedule the task
        event.container.schedule(self.interval_in_sec, self)


class PubSubMessagingHandler(MessagingHandler):

    def __init__(self, connector: Connector) -> None:
        """
        Base class acting a MessagingHandler to a `proton.Container`. Any custom handler should inherit from this class.

        :param connector: takes care of the connection .i.e TSL, SASL etc
        """
        MessagingHandler.__init__(self)

        self.connector = connector

        self.container = None
        self.connection = None

    def is_started(self):
        return self.container is not None and self.connection is not None

    def on_start(self, event: proton.Event):
        """
        Is triggered upon running the `proton.Container` that uses this handler. It creates a connection to the broker
        and can be overridden for further startup functionality.

        :param event:
        """
        self.container = event.container
        self.connection = self.connector.connect(self.container)
        _logger.info(f'Connected to broker @ {self.connector.url}')

    def create_sender_link(self, endpoint: str) -> proton.Sender:
        return self.container.create_sender(self.connection, endpoint)

    def create_receiver_link(self, endpoint: str) -> proton.Receiver:
        return self.container.create_receiver(self.connection, endpoint)

    @classmethod
    def create(cls,
               host: Optional[str] = None,
               cert_db: Optional[str] = None,
               cert_file: Optional[str] = None,
               cert_key: Optional[str] = None,
               cert_password: Optional[str] = None,
               sasl_user: Optional[str] = None,
               sasl_password: Optional[str] = None,
               allowed_mechs: Optional[str] = 'PLAIN'):
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
        if cert_db and cert_file and cert_key:
            # provide non empty password in case none is provided
            # somehow an empty password will lead to a failed connection even for non-password protected certificates
            cert_password = cert_password or ' '

            connector = TLSConnector(host, cert_db, cert_file, cert_key, cert_password)
        elif cert_db and sasl_user and sasl_password:
            connector = SASLConnector(host, cert_db, sasl_user, sasl_password, allowed_mechs)
        else:
            connector = Connector(host)

        return cls(connector=connector)

    @classmethod
    def create_from_config(cls, config: ConfigDict):
        """
        Factory method for creating an instance from config values

        :param config:
        :return: BrokerHandler
        """

        return cls.create(**config)


class Producer(PubSubMessagingHandler):

    def __init__(self, connector: Connector) -> None:
        """
        An implementation of a broker handler that is supposed to act as a publisher. It keeps a list of message
        producers which will generate respective messages which will be routed in the broker via a `proton.Sender`
        instance

        :param connector: takes care of the connection .i.e TSL, SASL etc
        """
        PubSubMessagingHandler.__init__(self, connector)

        self.endpoint: str = '/exchange/amq.topic'
        self._sender: Optional[proton.Sender] = None
        self.message_producers: Dict[str, Callable] = {}
        self.message_producer_timer_tasks: List[TimerTask] = []

    def on_start(self, event: proton.Event) -> None:
        """
        Is triggered upon running the `proton.Container` that uses this handler. If it has ScheduledMessageProducers
        items in its list they will be initialized and scheduled accordingly.

        :param event:
        """
        # call the parent event handler first to take care of the connection with the broker
        super().on_start(event)

        try:
            self._sender = self.create_sender_link(self.endpoint)
            _logger.debug(f"Created sender {self._sender}")
        except Exception as e:
            _logger.error(f'Error while creating sender: {str(e)}')
            return

        for timer_task in self.message_producer_timer_tasks:
            self._schedule_timer_task(timer_task)

    def add_message_producer(self, id: str, message_producer: Callable, interval_in_sec: Optional[int] = None) \
            -> None:

        if id in self.message_producers:
            raise ValueError(f"Message producer with id {id} already exists")

        self.message_producers[id] = message_producer

        if interval_in_sec is not None:
            message_producer_timer_task = self._make_message_producer_timer_task(id, interval_in_sec)
            if self.is_started():
                self._schedule_timer_task(message_producer_timer_task)
            else:
                self.message_producer_timer_tasks.append(message_producer_timer_task)

    def trigger_message_producer(self, message_producer_id: str, context: Optional[Any] = None) -> None:
        """
        Generates a message via the message_producer and sends it in the broker.

        :param message_producer_id:
        :param context:
        """
        if message_producer_id not in self.message_producers:
            raise ValueError(f"Invalid message producer id: {message_producer_id}")

        message_producer = self.message_producers[message_producer_id]
        try:
            message = message_producer(context=context)
        except MessageProducerError as e:
            _logger.error(f"Error while producing message for producer `{message_producer_id}`: {str(e)}")
            return

        _logger.info(f"Sending message for producer `{message_producer_id}`: {message}")
        self._send_message(message=message, subject=message_producer_id)

    def _make_message_producer_timer_task(self, message_producer_id: str, interval_in_sec: int) -> TimerTask:
        """

        :param message_producer_id:
        :param interval_in_sec:
        :return:
        """
        task = partial(self.trigger_message_producer, message_producer_id=message_producer_id)

        result = TimerTask(task=task, interval_in_sec=interval_in_sec)

        return result

    def _schedule_timer_task(self, timer_task: TimerTask) -> None:
        """

        :param timer_task:
        """
        self.container.schedule(timer_task.interval_in_sec, timer_task)

    def _send_message(self, message: proton.Message, subject: str) -> None:
        """
        Sends the provided message via the broker. The subject will serve as a routing key in the broker. Typically it
        should be the respective message_producer name.

        :param message:
        :param subject:
        """
        message.subject = subject

        if self._sender and self._sender.credit:
            try:
                _logger.info(message)
                _logger.info(message.body)
                _logger.info(message.content_type)
                self._sender.send(message)
                _logger.info("Message sent")
            except Exception as e:
                traceback.print_exc()
                _logger.error(f"Error while sending message: {str(e)}")
        else:
            _logger.info("No credit to send message {message}")


class Consumer(PubSubMessagingHandler):

    def __init__(self, connector: Connector) -> None:
        """
        An implementation of a broker client that is supposed to act as subscriber. It subscribes to queues of the
        broker by creating instances of `proton.Receiver` for each one of them.

        :param connector: takes care of the connection .i.e TSL, SASL etc
        """
        PubSubMessagingHandler.__init__(self, connector)

        # keep track of all the queues by receiver
        self.message_consumers_per_receiver: Dict[proton.Receiver, Tuple[str, Callable]] = {}

    def _get_receiver_by_queue(self, queue: str) -> proton.Receiver:
        """
        Find the receiver that corresponds to the given message_consumer.
        :param queue:
        :return:
        """
        for receiver, (receiver_queue, _) in self.message_consumers_per_receiver.items():
            if queue == receiver_queue:
                return receiver

    def attach_message_consumer(self, queue: str, message_consumer: Callable) -> None:
        """
        Create a new `proton.Receiver` and assign the message consumer to it

        :param queue:
        :param message_consumer: consumes the messages coming from its assigned queue in the broker.
        """
        receiver = self.create_receiver_link(queue)

        self.message_consumers_per_receiver[receiver] = (queue, message_consumer)

        _logger.debug(f"Created receiver {receiver} on queue {queue}")

    def detach_message_consumer(self, queue: str) -> None:
        """
        Remove the receiver that corresponds to the given queue.

        :param queue:
        """
        receiver = self._get_receiver_by_queue(queue)

        if not receiver:
            raise ValueError(f'No receiver found on queue: {queue}')

        # close the receiver
        receiver.close()

        _logger.debug(f"Closed receiver {receiver} on queue {queue}")

        # remove it from the list
        del self.message_consumers_per_receiver[receiver]

    def on_message(self, event: proton.Event) -> None:
        """
        Is triggered upon reception of messages via the broker.

        :param event:
        """
        queue, message_consumer = self.message_consumers_per_receiver[event.receiver]

        try:
            message_consumer(event.message)
        except MessageConsumerError as e:
            _logger.error(f"Error while processing message {event.message} on queue {queue}: {str(e)}")

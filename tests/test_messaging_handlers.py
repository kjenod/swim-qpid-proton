"""
Copyright 2020 EUROCONTROL
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
from unittest.mock import Mock

import proton
import pytest

from swim_proton.messaging_handlers import Producer, ScheduledTask, Consumer, PubSubMessagingHandler

__author__ = "EUROCONTROL (SWIM)"

# TODO: fix tests


def test_producer__on_start__error_while_creating_sender(caplog):
    caplog.set_level(logging.DEBUG)

    connector = Mock()
    mock_create_sender_link = Mock(side_effect=Exception('error'))
    mock_schedule_timer_task = Mock()
    message_producer = Mock()

    producer = Producer(connector)
    producer._schedule_task = mock_schedule_timer_task
    producer._create_sender_link = mock_create_sender_link
    producer.messengers = {'id': message_producer}

    producer.on_start(event=Mock())

    log_message = caplog.records[1].message

    mock_create_sender_link.assert_called_once()
    mock_schedule_timer_task.assert_not_called()
    assert "Error while creating sender: error" == log_message


def test_producer__on_start__no_errors(caplog):
    caplog.set_level(logging.DEBUG)

    sender = Mock()
    messenger = Mock()

    producer = Producer(connector=Mock())
    producer._schedule_messenger = Mock()
    producer._create_sender_link = Mock(return_value=sender)
    producer._to_schedule = [messenger]

    producer.on_start(event=Mock())

    log_message = caplog.records[1].message

    producer._create_sender_link.assert_called_once()
    producer._schedule_messenger.assert_called_once_with(messenger)
    assert f"Created sender: {sender}" == log_message


def test_producer__add_message_producer__without_interval():
    producer = Producer(connector=Mock())
    producer._schedule_task = Mock()

    message_producer = Mock()

    producer.add_messenger('id', message_producer)

    assert message_producer in producer.messengers.values()
    producer._schedule_task.assert_not_called()


@pytest.mark.parametrize('handler_is_started', [True, False])
def test_producer__add_message_producer__with_interval(handler_is_started):
    producer = Producer(connector=Mock())
    producer._schedule_task = Mock()
    producer.is_connected = Mock(return_value=handler_is_started)

    message_producer = Mock()

    producer.add_messenger('id', messenger=message_producer, interval_in_sec=5)

    assert message_producer in producer.messengers.values()

    if handler_is_started:
        producer._schedule_task.assert_called_once()
    else:
        producer._schedule_task.assert_not_called()
        assert 1 == len(producer.scheduled_tasks)


def test_producer__trigger_message_producer__message_producer_does_not_exist__raises_valueerror():
    producer = Producer(connector=Mock())

    with pytest.raises(ValueError) as e:
        producer.trigger_messenger('invalid_id')
    assert f"Invalid message producer id: invalid_id" == str(e.value)


def test_producer__trigger_message_producer__message_producer_error__does_not_send_message(caplog):
    caplog.set_level(logging.DEBUG)

    message_producer = Mock(side_effect=Exception('error'))

    producer = Producer(connector=Mock())
    producer._send_message = Mock()

    producer.messengers = {'id': message_producer}

    producer.trigger_messenger('id')

    log_message = caplog.records[0].message

    producer._send_message.assert_not_called()
    assert f"Error while producing message for producer `id`: error" == log_message


def test_producer__trigger_message_producer__no_errors__message_is_sent(caplog):
    caplog.set_level(logging.DEBUG)

    message = 'message'
    message_producer = Mock(return_value=message)

    producer = Producer(connector=Mock())
    producer._send_message = Mock()

    producer.messengers = {'id': message_producer}

    producer.trigger_messenger('id')

    log_message = caplog.records[0].message

    producer._send_message.assert_called_once_with(message=message, subject='id')
    assert f"Sending message for producer `id`: {message}" == log_message


def test_producer__make_message_producer_timer_task():
    producer = Producer(connector=Mock())

    message_producer_id = 'id'
    interval_in_sec = 5

    timer_task = producer._scheduled_task_from_messenger(message_producer_id, interval_in_sec)

    assert isinstance(timer_task, ScheduledTask)
    assert timer_task.interval_in_sec == interval_in_sec


def test_producer__send_message__no_credit__message_is_not_send(caplog):
    caplog.set_level(logging.DEBUG)

    producer = Producer(connector=Mock())
    producer._sender = Mock()
    producer._sender.send = Mock()
    producer._sender.credit = 0
    message = proton.Message(body='message')

    producer._send_message(message, 'subject')

    log_message = caplog.records[0].message

    producer._sender.send.assert_not_called()
    assert f"No credit to send message {message}" == log_message


def test_producer__send_message__error_while_sending__logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    producer = Producer(connector=Mock())
    producer._sender = Mock()
    producer._sender.send = Mock(side_effect=Exception('error'))
    producer._sender.credit = 1
    message = proton.Message(body='message')

    producer._send_message(message, 'subject')

    log_message = caplog.records[0].message
    assert f"Error while sending message: error" == log_message


def test_producer__send_message__no_errors__message_is_sent(caplog):
    caplog.set_level(logging.DEBUG)

    producer = Producer(connector=Mock())
    producer._sender = Mock()
    producer._sender.send = Mock()
    producer._sender.credit = 1
    message = proton.Message(body='message')

    producer._send_message(message, 'subject')

    log_message = caplog.records[0].message
    assert f"Message sent: {message}" == log_message


def test_consumer__get_endpoint_reg_by_receiver():
    consumer = Consumer(connector=Mock())
    receiver = Mock()
    endpoint = 'endpoint'
    consumer.endpoints_registry = {endpoint: (receiver, Mock())}

    assert endpoint == consumer._get_endpoint_reg_by_receiver(receiver)[0]
    assert consumer._get_endpoint_reg_by_receiver('invalid') is None


def test_consumer__attach_message_consumer(caplog):
    caplog.set_level(logging.DEBUG)

    consumer = Consumer(connector=Mock())
    receiver = Mock()
    endpoint = 'endpoint'
    message_consumer = Mock()
    consumer._create_receiver_link = Mock(return_value=receiver)
    consumer.is_connected = Mock(return_value=True)

    consumer.attach_message_consumer(endpoint, message_consumer)

    consumer._create_receiver_link.assert_called_once_with(endpoint)
    assert endpoint in consumer.endpoints_registry
    assert (receiver, message_consumer) == consumer.endpoints_registry[endpoint]

    log_message = caplog.records[0].message
    assert f"Created receiver {receiver} on endpoint {endpoint}" == log_message


def test_consumer__detach_message_consumer__receiver_not_found__raises_valueerror():
    consumer = Consumer(connector=Mock())

    with pytest.raises(KeyError) as e:
        consumer.detach_message_consumer('endpoint')


def test_consumer__detach_message_consumer__receiver_closes_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    consumer = Consumer(connector=Mock())
    receiver = Mock()
    receiver.close = Mock()
    endpoint = 'endpoint'
    consumer.endpoints_registry[endpoint] = (receiver, Mock())

    consumer.detach_message_consumer(endpoint)

    receiver.close.assert_called_once()

    log_message = caplog.records[0].message
    assert f"Closed receiver {receiver} on endpoint {endpoint}" == log_message


def test_consumer__on_message__message_consumer_error__logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    consumer = Consumer(connector=Mock())
    receiver = Mock()
    receiver.close = Mock()
    endpoint = 'endpoint'
    consumer.endpoints_registry[endpoint] = (receiver, Mock(side_effect=Exception('error')))

    event = Mock()
    event.receiver = receiver
    event.message = Mock()

    consumer.on_message(event)

    log_message = caplog.records[0].message
    assert f"Error while processing message {event.message} on endpoint {endpoint}: error" == log_message


@pytest.mark.parametrize('container, connection, is_started', [
    (None, None, False),
    (Mock(), None, False),
    (None, Mock(), False),
    (Mock(), Mock(), True)
])
def test_pubsubmessaging_handler__is_started(container, connection, is_started):
    messaging_handler = PubSubMessagingHandler(connector=Mock())
    messaging_handler.container = container
    messaging_handler.connection = connection

    assert is_started == messaging_handler.is_connected()


def test_timer_task__on_timer_task__task_is_called_and_rescheduled():
    task = Mock()
    interval_in_sec = 5

    timer_task = ScheduledTask(task=task, interval_in_sec=interval_in_sec)

    event = Mock()
    event.container = Mock()
    event.container.schedule = Mock()

    timer_task.on_timer_task(event)

    task.assert_called_once()

    event.container.schedule.assert_called_once_with(interval_in_sec, timer_task)

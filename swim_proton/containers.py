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
import threading
from typing import Optional

from proton.reactor import Container


__author__ = "EUROCONTROL (SWIM)"

from swim_proton import ConfigDict
from swim_proton.messaging_handlers import PubSubMessagingHandler


class PubSubContainer:

    def __init__(self, messaging_handler: PubSubMessagingHandler):
        """
        Wraps up a proton.reactor.Container and enables run in thread mode
        :param messaging_handler:
        """
        self.messaging_handler: PubSubMessagingHandler = messaging_handler
        self.config: Optional[ConfigDict] = None
        self._thread: Optional[threading.Thread] = None
        self._container: Optional[Container] = None

    def is_running(self):
        """
        Determines whether the container is running by checking the handler and the underlying thread if it's running in
        threaded mode.
        :return:
        """
        if self._thread:
            return self.messaging_handler.is_started() and self._thread.is_alive()
        else:
            return self.messaging_handler.is_started()

    def run(self, threaded: bool = False):
        """
        Runs the container in threaded or not mode
        :param threaded:
        :return:
        """
        if threaded and not self.is_running():
            self._thread = threading.Thread(target=self._run)
            self._thread.daemon = True

            return self._thread.start()

        return self._run()

    def _run(self):
        """
        The actual runner
        """
        self._container = Container(self.messaging_handler)
        self._container.run()

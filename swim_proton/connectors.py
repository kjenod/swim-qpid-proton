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

__author__ = "EUROCONTROL (SWIM)"

import proton
from proton import SSLDomain
from proton.reactor import Container


class Connector:
    protocol = 'amqp'

    def __init__(self, host: str) -> None:
           print("----- 1 ----- def __init__(self, host: str) -> None: " )

        """
        Handles the connection to a broker without authentication
        :param host:
        """
        if host is None:
            raise ValueError('no host was provided')

        self.host = host

    @property
    def url(self):
           print("----- 2 ----- def url(self): " )

        if self.host.startswith('amqp'):
            return self.host

        return f"{self.protocol}://{self.host}"

    def connect(self, container: Container) -> proton.Connection:
        return container.connect(self.url)


class TLSConnector(Connector):
    protocol = 'amqps'

    def __init__(self, host: str, cert_db: str, cert_file: str, cert_key: str, cert_password: str) \
            -> None:
            print("----- 3 ----- def __init__(self, host: str, cert_db: str, cert_file: str, cert_key: str, cert_password: str) " )
           
        """
        Handles the connection to a broker via the TSL layer
        :param host:
        :param cert_db:
        :param cert_file:
        :param cert_key:
        :param cert_password:
        """
        super().__init__(host)

        self.cert_file = cert_file
        self.cert_key = cert_key
        self.cert_db = cert_db
        self.cert_password = cert_password

    def connect(self, container: Container) -> proton.Connection:
    print("----- 4 ----- connect(self, container: Container) " )
        container = self._prepare_container(container)

        return container.connect(self.url)

    def _prepare_container(self, container: Container) -> Container:
    print("----- 5 ----- def _prepare_container(self, container: Container)  " )
        container.ssl.client.set_trusted_ca_db(self.cert_db)
        container.ssl.client.set_peer_authentication(SSLDomain.VERIFY_PEER)
        container.ssl.client.set_credentials(self.cert_file,
                                             self.cert_key,
                                             self.cert_password)

        return container


class SASLConnector(Connector):
    protocol = 'amqps'

    def __init__(self, host, user: str, password: str, cert_db: str, allowed_mechs: str) -> None:
        print("----- 6 ----- __init__(self, host, user: str, password: str, cert_db: str, allowed_mechs: str)  " )

        """
        Handles the connection to a broker via the SASL layer

        :param host:
        :param cert_db:
        :param user:
        :param password:
        :param allowed_mechs:
        """
        super().__init__(host)
        self.user = user
        self.password = password
        self.cert_db = cert_db
        self.allowed_mechs = allowed_mechs

    def _prepare_container(self, container: Container) -> Container:
    print("----- 7 ----- _prepare_container(self, container: Container)  " )
        container.ssl.client.set_trusted_ca_db(self.cert_db)
        container.ssl.client.set_peer_authentication(SSLDomain.VERIFY_PEER)

        return container

    def connect(self, container: Container) -> proton.Connection:
        print("----- 8 ----- connect(self, container: Container) " )

        container = self._prepare_container(container)

        return container.connect(self.url,
                                 sasl_enabled=True,
                                 allowed_mechs=self.allowed_mechs,
                                 user=self.user,
                                 password=self.password)

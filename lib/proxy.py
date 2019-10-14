#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Filename : proxy
    Date: 09.05.2019 06:51
    Project: pyRsyncBackup
    AUTHOR : Sergey Utkin
    
    Copyright 2019 Sergey Utkin utkins01@gmail.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   https://pypi.org/project/sshtunnel/
"""
import queue
import socketserver
import sys
import threading
from binascii import hexlify
from select import select

from sshtunnel import address_to_str

import database as rb_db
import log as rb_log
import error as rb_error
import paramiko
import socket
import uuid

DAEMON = True
TUNNEL_TIMEOUT = 1.0  #: Timeout (seconds) for tunnel connection


def get_bind_local(channel):
    return channel.local_address


def get_connection_id():
    return str(uuid.uuid4())


class _ForwardHandler(socketserver.BaseRequestHandler):
    """ Base handler for tunnel connections """
    remote_address = None
    ssh_transport = None  # type: paramiko.Transport
    logger = None
    info = None

    def _redirect(self, chan):
        while chan.active:
            rqst, _, _ = select([self.request, chan], [], [], 5)
            if self.request in rqst:
                data = self.request.recv(1024)
                if not data:
                    break
                self.logger.debug('>>> OUT {0} send to {1}: {2} >>>'.format(
                    self.info,
                    self.remote_address,
                    hexlify(data)
                ))
                chan.send(data)
            if chan in rqst:  # else
                if not chan.recv_ready():
                    break
                data = chan.recv(1024)
                self.logger.debug(
                    '<<< IN {0} recv: {1} <<<'.format(self.info, hexlify(data))
                )
                self.request.send(data)

    def handle(self):
        uid = get_connection_id()
        self.info = '#{0} <-- {1}'.format(uid, self.client_address or
                                          self.server.local_address)
        src_address = self.request.getpeername()
        if not isinstance(src_address, tuple):
            src_address = ('dummy', 12345)
        try:
            chan = self.ssh_transport.open_channel(
                kind='direct-tcpip',
                dest_addr=self.remote_address,
                src_addr=src_address,
                timeout=TUNNEL_TIMEOUT
            )
        except paramiko.SSHException:
            chan = None
        if chan is None:
            msg = '{0} to {1} was rejected by the SSH server'.format(
                self.info,
                self.remote_address
            )
            self.logger.error(msg)
            raise rb_error.RBError(msg)

        self.logger.info('{0} connected'.format(self.info))
        try:
            self._redirect(chan)
        except socket.error:
            # Sometimes a RST is sent and a socket error is raised, treat this
            # exception. It was seen that a 3way FIN is processed later on, so
            # no need to make an ordered close of the connection here or raise
            # the exception beyond this point...
            self.logger.error('{0} sending RST'.format(self.info))
        except Exception as e:
            self.logger.error('{0} error: {1}'.format(self.info, repr(e)))
        finally:
            chan.close()
            self.request.close()
            self.logger.info('{0} connection closed.'.format(self.info))


class _ForwardServer(socketserver.TCPServer):  # Not Threading
    """
    Non-threading version of the forward server
    """
    allow_reuse_address = True  # faster rebinding

    def __init__(self, *args, **kwargs):
        self.tunnel_ok = queue.Queue()
        socketserver.TCPServer.__init__(self, *args, **kwargs)

    def handle_error(self, request, client_address):
        (exc_class, exc, tb) = sys.exc_info()
        self.tunnel_ok.put(False)

    @property
    def local_address(self):
        return self.server_address

    @property
    def local_host(self):
        return self.server_address[0]

    @property
    def local_port(self):
        return self.server_address[1]

    @property
    def remote_address(self):
        return self.RequestHandlerClass.remote_address

    @property
    def remote_host(self):
        return self.RequestHandlerClass.remote_address[0]

    @property
    def remote_port(self):
        return self.RequestHandlerClass.remote_address[1]


class _ThreadingForwardServer(socketserver.ThreadingMixIn, _ForwardServer):
    """
    Allow concurrent connections to each tunnel
    """
    # If True, cleanly stop threads created by ThreadingMixIn when quitting
    daemon_threads = DAEMON


class Tunnel:
    ssh_forward_server = ...  # type: _ThreadingForwardServer
    is_alive = False
    logger = None  # type: rb_log.Log
    local_address = None

    def __init__(self, dst_host=None, dst_port=None, ssh_transport=None, logger=None):
        """
        :type dst_host: str
        :type dst_port: int
        :type ssh_transport: paramiko.Transport
        """
        if not dst_host or not dst_port or not ssh_transport:
            raise rb_error.RBError('Ошибка инициализации тунеля!!!')
        if dst_port < 0 or dst_port > 65535:
            raise rb_error.RBError('Не корректное значение dst_port!!!')

        self.dst_port = dst_port
        self.dst_host = dst_host
        self.transport = ssh_transport
        self.logger = logger

    def _create_handler(self):
        class Handler(_ForwardHandler):
            remote_address = (self.dst_host, self.dst_port)
            ssh_transport = self.transport
            logger = self.logger

        return Handler

    def start(self):
        handler = self._create_handler()
        self.ssh_forward_server = _ThreadingForwardServer(('127.0.0.1', 0), handler)
        self.ssh_forward_server.daemon_threads = DAEMON
        self.local_address = self.ssh_forward_server.server_address

        thread = threading.Thread(
            target=self._serve_forever_wrapper,
            args=(self.ssh_forward_server,),
            name='Srv-{0}'.format(address_to_str(self.ssh_forward_server.local_port))
        )
        thread.daemon = DAEMON
        thread.start()

    def stop(self):
        self.ssh_forward_server.shutdown()
        self.ssh_forward_server.server_close()

    def _serve_forever_wrapper(self, _srv, poll_interval=0.1):
        """
        Wrapper for the server created for a SSH forward
        """
        self.logger.info('Opening tunnel: {0} <> {1}'.format(
            address_to_str(_srv.local_address),
            address_to_str(_srv.remote_address))
        )
        _srv.serve_forever(poll_interval)  # blocks until finished

        self.logger.info('Tunnel: {0} <> {1} released'.format(
            address_to_str(_srv.local_address),
            address_to_str(_srv.remote_address))
        )


class Proxy:
    def __init__(self, proxy, logger):
        """
        Класс прокси серверов.
        :type proxy: rb_db.Proxy
        :type logger: rb_log.Log
        """
        self.ip = proxy.ip
        self.port = proxy.port
        self.login = proxy.login
        self.password = proxy.password
        self.client = paramiko.SSHClient()
        self.client.set_log_channel(logger.logger.name)
        self.logger = logger

    def start(self):
        self.client.load_system_host_keys()
        try:
            self.client.connect(self.ip, port=self.port, username=self.login, password=self.password, timeout=15)
        except (TimeoutError, socket.timeout) as error:
            raise rb_error.RBError("Proxy [{}] - {}".format(self.ip, error))

    def is_active(self):
        t = self.client.get_transport()  # type: paramiko.Transport
        if t is not None:
            return t.is_active()
        return False

    def open_forwarding_channel(self, destination_ip, destination_port, logging: rb_log.Log):
        tunnel = Tunnel(dst_host=destination_ip,
                        dst_port=destination_port,
                        ssh_transport=self.client.get_transport(),
                        logger=logging)
        tunnel.start()
        return tunnel

    def close_forwarding_channel(self, tunnel):
        self.logger.debug("Proxy[{}]: close channel {}".format(self.ip, tunnel.local_address))
        tunnel.stop()
        del tunnel

    def stop(self):
        self.client.close()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplementedError

        return self.ip == other.ip

    def __hash__(self):
        return hash(self.ip)

    def __repr__(self):
        return "{0}".format(self.__dict__)

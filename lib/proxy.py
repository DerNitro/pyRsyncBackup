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
"""

import database as rb_db
import log as rb_log
import error as rb_error
import sshtunnel


class Proxy:
    def __init__(self, proxy, host, logging):
        self.proxy = proxy          # type: rb_db.Proxy
        self.logging = logging      # type: rb_log.Log
        self.host = host            # type: rb_db.Host

        self.client = sshtunnel.SSHTunnelForwarder(
            (self.proxy.ip,self.proxy.port),
            ssh_username=self.proxy.login,
            ssh_password=self.proxy.password,
            remote_bind_address=(self.host.ip, self.host.port),
            local_bind_address=('127.0.0.1',),
            logger=logging.logger
        )

    def start(self):
        try:
            self.client.start()
        except sshtunnel.HandlerSSHTunnelForwarderError:
            rb_error.RBError('Ошибка подключения к прокси серверу - {proxy.ip}'.format(proxy=self.proxy))
        except sshtunnel.BaseSSHTunnelForwarderError:
            rb_error.RBError('Ошибка подключения к прокси серверу - {proxy.ip}'.format(proxy=self.proxy))

    def get_port_forward_info(self):
        return self.client.local_bind_address

    def is_active(self):
        return self.client.is_active

    def stop(self):
        self.client.stop()

    def __del__(self):
        pass

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Filename : config
    Date: 25.03.2018 09:57
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
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


def calc_size(size):
    try:
        return int(size)
    except ValueError:
        pass

    postfix = size[-1]
    value = size[:-1]
    if str(postfix) == 'K':     # kilobyte -> byte
        result = int(value) * 1024
    elif str(postfix) == 'M':   # megabyte -> byte
        result = int(value) * 1024 * 1024
    elif str(postfix) == 'G':   # gigabyte -> byte
        result = int(value) * 1024 * 1024 * 1024
    elif str(postfix) == 'm':   # minute -> seconds
        result = int(value) * 60
    elif str(postfix) == 'h':   # hour -> seconds
        result = int(value) * 60 * 60
    elif str(postfix) == 'd':   # day -> seconds
        result = int(value) * 60 * 60 * 24
    else:
        raise ValueError

    return result


def str2bool(value):
    if str(value).lower() in ['true', '1', 't', 'y', 'yes']:
        return True
    else:
        return False


class AppConfiguration:
    HostList = False
    DataBaseFile = False
    BackupInterval = 3600
    Threads = 5
    log = {}

    def __init__(self, config_file):
        self.conf = ConfigParser()
        self.conf.read(config_file)

        self.log['dir'] = self.conf.get("Logging", "Dir", fallback="/var/log/pyRsyncBackup/")
        self.log['level'] = self.conf.get("Logging", "Level", fallback="INFO")
        self.log['count'] = self.conf.getint("Logging", "Count", fallback=10)
        self.log['size'] = calc_size(self.conf.get("Logging", "Size", fallback="10M"))

        self.HostList = self.conf.get("Main", "HostList", fallback=False)
        self.Threads = self.conf.getint("Main", "Threads", fallback=5)

        self.DbHost = self.conf.get("DataBase", "Host", fallback='localhost')
        self.DbPort = self.conf.get("DataBase", "Port", fallback=5432)
        self.DbBase = self.conf.get("DataBase", "DataBase", fallback='pyRsyncBackup')
        self.DbLogin = self.conf.get("DataBase", "Login", fallback='pyRsyncBackup')
        self.DbPassword = self.conf.get("DataBase", "Password", fallback='123456')

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def load_modules(self, engine):
        for item in self.conf.sections():
            if item == 'Main' or item == 'Logging' or item == 'DataBase':
                pass
            else:
                module = rb_db.Module()
                module.name = item
                module.path = self.conf.get(item, 'path')
                module.include = self.conf.get(item, 'include', fallback=None)
                module.exclude = self.conf.get(item, 'exclude', fallback=None)
                with rb_db.edit(engine) as db:
                    db.add(module)
        pass

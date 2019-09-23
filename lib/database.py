#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Filename : database
    Date: 29.03.2019 07:45
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
import datetime

import sqlalchemy
import sqlalchemy.exc as sql_exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
from configparser import ConfigParser, MissingSectionHeaderError, DuplicateOptionError

import error as rb_error

Base = declarative_base()


@contextmanager
def select(engine):
    cl_session = sessionmaker(bind=engine)
    session = cl_session()
    yield session
    session.close()


@contextmanager
def edit(engine):
    cl_session = sessionmaker(bind=engine)
    session = cl_session()
    yield session
    session.commit()
    session.close()


class Module(Base):
    __tablename__ = "module"
    name = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    path = sqlalchemy.Column(sqlalchemy.String)
    exclude = sqlalchemy.Column(sqlalchemy.String)
    include = sqlalchemy.Column(sqlalchemy.String)
    disabled = sqlalchemy.Column(sqlalchemy.Boolean, default=False)

    def __repr__(self):
        return "{0}".format(self.__dict__)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplementedError

        return self.name == other.name and self.path == other.path

    def __hash__(self):
        return hash(self.name) ^ hash(self.path)


class Host(Base):
    __tablename__ = "host"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    name = sqlalchemy.Column(sqlalchemy.String)
    ip = sqlalchemy.Column(sqlalchemy.String)
    port = sqlalchemy.Column(sqlalchemy.Integer, default=873)
    backup_directory = sqlalchemy.Column(sqlalchemy.String, default=None)
    backup_interval = sqlalchemy.Column(sqlalchemy.String, default=None)
    discovering_interval = sqlalchemy.Column(sqlalchemy.String, default=None)
    proxy = sqlalchemy.Column(sqlalchemy.Integer, default=None)
    backup_date = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.datetime.now)
    discovering_date = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.datetime.now)
    disabled = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    user = sqlalchemy.Column(sqlalchemy.String, default=None)
    password = sqlalchemy.Column(sqlalchemy.String, default=None)

    def __repr__(self):
        return "{0}".format(self.__dict__)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplementedError

        return self.ip == other.ip and self.proxy == other.proxy

    def __hash__(self):
        return hash(self.ip) ^ hash(self.proxy)

    def load(self, v):
        v = str(v).replace(" ", "").split(',')
        for values in v:
            key, val = str(values).split("=")
            if key == 'ip':
                self.ip = val
            elif key == 'DiscoveringInterval':
                self.discovering_interval = val
            elif key == 'port':
                self.port = val
            elif key == 'BackupInterval':
                self.backup_interval = val
            elif key == 'BackupDirectory':
                self.backup_directory = val
            elif key == 'PasswordFile':
                self.password = val
            elif key == 'User':
                self.user = val


class Proxy(Base):
    __tablename__ = "proxy"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    ip = sqlalchemy.Column(sqlalchemy.String)
    port = sqlalchemy.Column(sqlalchemy.Integer, default=22)
    login = sqlalchemy.Column(sqlalchemy.String)
    password = sqlalchemy.Column(sqlalchemy.String)
    create_date = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.datetime.now())

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplementedError

        return self.ip == other.ip

    def __hash__(self):
        return hash(self.ip)

    def __repr__(self):
        return "{0}".format(self.__dict__)


class ActiveModules(Base):
    __tablename__ = "active_modules"
    host = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    module = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    discovering_date = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.datetime.now)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplementedError

        return self.host == other.host and self.module == other.module

    def __hash__(self):
        return hash(self.host) ^ hash(self.module)

    def __repr__(self):
        return "{0}".format(self.__dict__)


def create(engine):
    Base.metadata.create_all(engine)


def check_database(engine):
    """
    Проверка базы данных
    :type engine: sqlalchemy.create_engine
    """
    # Тестирование подключения к Базе данных
    try:
        connection = engine.connect()
        connection.execute("select datname from pg_database;")
        connection.close()
    except sql_exc.DisconnectionError, sql_exc.TimeoutError:
        raise rb_error.RBError('Ошибка подключения к базе данных: {}'.format(engine))
    except sql_exc.NoSuchTableError:
        raise rb_error.RBError('Ошибка при проверке базы данных: {}'.format(engine))

    # Создание структуры приложения
    create(engine)

    # Очищаем таблицы и сбрасываем seq
    with edit(engine) as dbe:
        dbe.query(Proxy).delete()
        dbe.query(ActiveModules).delete()
        dbe.query(Host).delete()
        dbe.query(Module).delete()
        dbe.execute("ALTER SEQUENCE host_id_seq RESTART WITH 1;")
        dbe.execute("ALTER SEQUENCE proxy_id_seq RESTART WITH 1;")


def import_host(engine, conf):
    try:
        config = ConfigParser()
        config.read(conf)
    except MissingSectionHeaderError:
        raise rb_error.RBError('Ошибка чтения файла: {file}'.format(file=conf))
    except DuplicateOptionError:
        raise rb_error.RBError('Найдены дубли хостов: {file}'.format(file=conf))

    backup_directory = config.get('Main', 'BackupDirectory', fallback=None)
    backup_interval = config.get('Main', 'BackupInterval', fallback=None)
    discovering_interval = config.get('Main', 'DiscoveringInterval', fallback=None)
    user = config.get('Main', 'User', fallback=None)
    password_file = config.get('Main', 'PasswordFile', fallback=None)
    proxy_id = None

    if config.has_section('Proxy'):
        proxy = Proxy()
        proxy.ip = config.get('Proxy', 'ip', fallback=None)
        proxy.port = config.get('Proxy', 'port', fallback=22)
        proxy.login = config.get('Proxy', 'login', fallback=None)
        proxy.password = config.get('Proxy', 'password', fallback=None)

        if not proxy.ip:
            raise rb_error.RBError('Не полная информация о Proxy сервере: {file}'.format(file=conf))
        else:
            with edit(engine) as db:
                db.add(proxy)
                db.flush()
                db.refresh(proxy)
                proxy_id = proxy.id
    if not config.has_section('Host'):
        raise rb_error.RBError('Отсутствует секция "Host" в конфигурационном файле: {file}'.format(file=conf))

    for item in config.items('Host', True):
        host = Host()
        host.name = item[0]
        host.backup_directory = backup_directory
        host.backup_interval = backup_interval
        host.discovering_interval = discovering_interval
        host.proxy = proxy_id
        host.user = user
        host.password = password_file
        host.load(item[1])
        with edit(engine) as db:
            db.add(host)

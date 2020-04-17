#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
    Filename : pyRsyncBackup
    Date: 29.03.2019 06:04
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
import sys
import os
import time
from multiprocessing import Pool
import signal
import socket
from contextlib import closing
from sqlalchemy import create_engine
import subprocess
from contextlib import contextmanager

# App Lib
run_dir_name, run_file_name = os.path.split(os.path.abspath(__file__))
sys.path.append(os.path.join(run_dir_name, 'lib'))
import config as rb_conf
import log as rb_log
import database as rb_db
import error as rb_error
import proxy as rb_proxy

__author__ = 'Sergey Utkin'
__email__ = 'utkins01@gmail.com'
__status__ = "Development"
__version__ = "0.2"
__maintainer__ = "Sergey Utkin"
__copyright__ = "Copyright 2019, Sergey Utkin"
__program__ = 'pyRsyncBackup'

global appLogging
__timeout__ = 600


def handle_sig_term(signum, frame):
    global interrupted
    if signum != 15:
        appLogging.debug('Получен сигнал на завершение приложения!!!({},{})'.format(signum, frame))
    interrupted = True


def app_exit(code):
    appLogging.info('Завершение приложения')
    sys.exit(code)


def alive_host(ip, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(3)
        if sock.connect_ex((ip, port)) == 0:
            return True
        else:
            return False


def popen_timeout(cmd, timeout, **kwargs):
    run = subprocess.Popen(cmd.split(), **kwargs)
    for i in range(timeout):
        if run.poll() is not None:
            return run.returncode, run.communicate()
        time.sleep(1)
    run.kill()
    raise rb_error.KillTimeout('Kill process timeout pid - {}'.format(run.pid))


def discovering(host):
    if interrupted:
        appLogging.debug('Discovering - {host.name} skip.'.format(host=host))
        return False

    discover_engine = create_engine(
        'postgresql://{c.DbLogin}:{c.DbPassword}@{c.DbHost}:{c.DbPort}/{c.DbBase}'.format(c=appConfiguration))

    appLogging.debug('Discovering - {host.name}.'.format(host=host))
    tunnel = None

    with rb_db.edit(discover_engine) as dbe:
        discovering_interval = rb_conf.calc_size(host.discovering_interval)
        dbe.query(rb_db.Host).filter(rb_db.Host.id == host.id).update(
            {rb_db.Host.discovering_date: datetime.datetime.now() + datetime.timedelta(seconds=discovering_interval)}
        )

    rsync_dry_run = '/usr/bin/rsync --dry-run --timeout=15 '
    if host.user:
        source = 'rsync://{host.user}@{host.ip}:{host.port}{module.path}'
    else:
        source = 'rsync://{host.ip}:{host.port}{module.path}'
    if host.password:
        rsync_dry_run += '--password-file {host.password} '
    rsync_dry_run += source
    if not os.path.isdir(os.path.join(appConfiguration.log['dir'], 'hosts')):
        os.makedirs(os.path.join(appConfiguration.log['dir'], 'hosts'))
    host_logging = rb_log.Log(host.name,
                              os.path.join(appConfiguration.log['dir'], 'hosts', host.name + '.log'),
                              appConfiguration.log['level'],
                              appConfiguration.log['count'],
                              appConfiguration.log['size']
                              )

    if host.proxy:
        with rb_db.select(discover_engine) as db:
            proxy = db.query(rb_db.Proxy).filter(rb_db.Proxy.id == host.proxy).one()

        if not alive_host(proxy.ip, proxy.port):
            appLogging.warning('Хост {host.name} прокси сервер {proxy.ip} - не доступен!!!'
                               .format(proxy=proxy, host=host))
            return False

        tunnel = rb_proxy.Proxy(proxy, host, host_logging)
        try:
            tunnel.start()
        except rb_error.RBError as err:
            appLogging.error(err)
            del host_logging
            return False

        if tunnel.is_active():
            host.ip, host.port = tunnel.get_port_forward_info()
        else:
            appLogging.error('Хост {host.name} прокси сервер {proxy.ip} - Ошибка формирования тунеля!!!'
                             .format(proxy=proxy, host=host))
            del host_logging
            return False

    if alive_host(host.ip, host.port):
        host_logging.debug('Хост: {host.name}({host.ip}, {host.port}) - доступен.'.format(host=host))
        with rb_db.edit(discover_engine) as dbe:
            dbe.query(rb_db.ActiveModules).filter(rb_db.ActiveModules.host == host.id).delete()
        with rb_db.select(discover_engine) as db:
            modules = db.query(rb_db.Module).all()
        for module in modules:
            if interrupted:
                break

            host_logging.debug('run discovering: {cmd}'.format(cmd=rsync_dry_run.format(host=host, module=module)))
            try:
                run, _ = popen_timeout(
                    rsync_dry_run.format(host=host, module=module),
                    __timeout__,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            except rb_error.KillTimeout as err:
                host_logging.error('Хост: {host.name} - '.format(host=host) + str(err))
                break
            except:
                host_logging.error('Хост: {host.name} - error subprocess.Popen'.format(host=host))
                break
            if run == 0:
                host_logging.debug('Хост: {host.name} - найден модуль {module.name}'.format(module=module, host=host))
                with rb_db.edit(discover_engine) as dbe:
                    dbe.add(rb_db.ActiveModules(host=host.id, module=module.name))
            else:
                host_logging.debug('Хост: {host.name} - нет модуля {module.name}'.format(module=module, host=host))

            del run
    else:
        host_logging.warning('Хост: {host} - не доступен!'.format(host=host.name))

    if isinstance(tunnel, rb_proxy.Proxy):
        tunnel.stop()

    del host_logging, tunnel


def backup(host):
    if interrupted:
        appLogging.debug('Backup - {host.name} skip.'.format(host=host))
        return False

    backup_engine = create_engine(
        'postgresql://{c.DbLogin}:{c.DbPassword}@{c.DbHost}:{c.DbPort}/{c.DbBase}'.format(c=appConfiguration))

    tunnel = None
    appLogging.debug('Backup - {host.name}.'.format(host=host))

    with rb_db.edit(backup_engine) as dbe:
        backup_interval = rb_conf.calc_size(host.backup_interval)
        dbe.query(rb_db.Host).filter(rb_db.Host.id == host.id).update(
            {rb_db.Host.backup_date: datetime.datetime.now() + datetime.timedelta(seconds=backup_interval)}
        )

    command = '/usr/bin/rsync -aclk --timeout=15 --ignore-errors --delete --backup --backup-dir {backup_dir} '
    if host.user:
        source = 'rsync://{host.user}@{host.ip}:{host.port}{module.path} '
    else:
        source = 'rsync://{host.ip}:{host.port}{module.path} '
    destination = '{host.backup_directory}/{host.name}/{module.name}'

    if host.password:
        command += '--password-file {host.password} '

    if not os.path.isdir(os.path.join(appConfiguration.log['dir'], 'hosts')):
        os.makedirs(os.path.join(appConfiguration.log['dir'], 'hosts'))
    host_logging = rb_log.Log(host.name,
                              os.path.join(appConfiguration.log['dir'], 'hosts', host.name + '.log'),
                              appConfiguration.log['level'],
                              appConfiguration.log['count'],
                              appConfiguration.log['size']
                              )

    if host.proxy:
        with rb_db.select(backup_engine) as db:
            proxy = db.query(rb_db.Proxy).filter(rb_db.Proxy.id == host.proxy).one()

        if not alive_host(proxy.ip, proxy.port):
            appLogging.warning('Хост {host.name} прокси сервер {proxy.ip} - не доступен!!!'
                               .format(proxy=proxy, host=host))
            del host_logging
            return False

        tunnel = rb_proxy.Proxy(proxy, host, host_logging)
        try:
            tunnel.start()
        except rb_error.RBError as err:
            appLogging.error(err)
            del host_logging
            return False

        if tunnel.is_active():
            host.ip, host.port = tunnel.get_port_forward_info()
        else:
            appLogging.error('Хост {host.name} прокси сервер {proxy.ip} - Ошибка формирования тунеля!!!'
                             .format(proxy=proxy, host=host))
            del host_logging
            return False

    if alive_host(host.ip, host.port):
        host_logging.debug('Хост: {host.name}({host.ip}, {host.port}) - доступен.'.format(host=host))
        with rb_db.select(backup_engine) as db:
            active_modules = db.query(rb_db.ActiveModules).filter(rb_db.ActiveModules.host == host.id).all()

        for active_module in active_modules:
            if interrupted:
                break
            with rb_db.select(backup_engine) as db:
                module = db.query(rb_db.Module).filter(rb_db.Module.name == active_module.module).one()
            backup_dir = destination.format(host=host, module=module) + datetime.datetime.now().strftime(
                '/%Y-%m-%d-%H-%M-%S')
            if not os.path.isdir(destination.format(host=host, module=module)):
                os.makedirs(destination.format(host=host, module=module))
            rsync = command
            if module.exclude:
                if os.path.isfile(module.exclude):
                    rsync += "--exclude-from {0} ".format(module.exclude)
                else:
                    for exclude in str(module.exclude).split(','):
                        rsync += "--exclude {0} ".format(exclude.strip())
            if module.include:
                if os.path.isfile(module.include):
                    rsync += "--include-from {0} ".format(module.include)
                else:
                    for include in str(module.include).split(','):
                        rsync += "--include {0} ".format(include.strip())

            rsync += source + destination + '/current'
            host_logging.debug('run command: {rsync}'.format(rsync=rsync.format(host=host,
                                                                                module=module,
                                                                                backup_dir=backup_dir)))
            try:
                run, res = popen_timeout(
                    rsync.format(host=host, module=module, backup_dir=backup_dir),
                    __timeout__,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            except rb_error.KillTimeout as err:
                host_logging.error('Хост: {host.name} - '.format(host=host) + str(err))
                break
            except:
                host_logging.error('Хост: {host.name} - error subprocess.Popen')
                break
            if run == 0:
                host_logging.info(
                    'Хост: {host.name} - успешное резервное копирование {module.name}'.format(module=module, host=host))
            else:
                host_logging.warning(
                    'Хост: {host.name} - Ошибка резервного копирования {module.name}\n'
                    '{res}'.format(module=module, host=host, res=res))

            del res, run
            if os.path.isdir(backup_dir):
                if len(os.listdir(backup_dir)) == 0:
                    os.rmdir(backup_dir)

    else:
        host_logging.warning('Хост: {host} - не доступен!'.format(host=host.name))

    if isinstance(tunnel, rb_proxy.Proxy):
        tunnel.stop()

    del host_logging, tunnel


@contextmanager
def pool_context(*args, **kwargs):
    pool = Pool(*args, **kwargs)
    yield pool
    pool.terminate()


signal.signal(signal.SIGTERM, handle_sig_term)
signal.signal(signal.SIGINT, handle_sig_term)

appConfiguration = rb_conf.AppConfiguration('/etc/pyRsyncBackup/pyRsyncBackup.conf')
if not os.path.isdir(appConfiguration.log['dir']):
    try:
        os.mkdir(appConfiguration.log['dir'])
    except OSError as e:
        print('Ошибка создания директории лог файлов')
        print(e)
        sys.exit(1)

appLogging = rb_log.Log(__program__,
                        os.path.join(appConfiguration.log['dir'], 'pyRsyncBackup.log'),
                        appConfiguration.log['level'],
                        appConfiguration.log['count'],
                        appConfiguration.log['size'])

appLogging.info('Запуск приложения {program} {version}. PID:{pid}'
                .format(program=__program__, version=__version__, pid=os.getpid()))

if not os.path.isfile('/usr/bin/rsync'):
    appLogging.critical('Отсутствует исполняемый файл /usr/bin/rsync!!!')
    app_exit(1)

if not appConfiguration.HostList:
    appLogging.critical('Отсутствует значение директории с конфигурацией узлов!!!')
    app_exit(1)

engine = create_engine(
    'postgresql://{c.DbLogin}:{c.DbPassword}@{c.DbHost}:{c.DbPort}/{c.DbBase}'.format(c=appConfiguration))

try:
    rb_db.check_database(engine)
except rb_error.RBError as error:
    appLogging.critical(error)
    app_exit(1)

for config_file in os.listdir(appConfiguration.HostList):
    if os.path.split(config_file)[-1].split('.')[-1] in ["cfg", "conf"]:
        try:
            appLogging.debug('Инициализация конфигурации: {file}'.format(
                file=os.path.join(appConfiguration.HostList, config_file)))
            rb_db.import_host(engine, os.path.join(appConfiguration.HostList, config_file))
        except rb_error.RBError as e:
            appLogging.warning(e)

appConfiguration.load_modules(engine)
appLogging.debug('Инициализация завершена.')

interrupted = False
discovering_list = []
backup_list = []

while True:
    time.sleep(1)
    del discovering_list
    with rb_db.select(engine) as dbs:
        discovering_list = dbs.query(rb_db.Host).filter(rb_db.Host.discovering_date < datetime.datetime.now()).all()

    if len(discovering_list) > 0:
        with pool_context(processes=appConfiguration.Threads) as p:
            p.map(discovering, discovering_list)

    time.sleep(1)

    del backup_list

    with rb_db.select(engine) as dbs:
        backup_list = dbs.query(rb_db.Host).filter(rb_db.Host.backup_date < datetime.datetime.now()).all()

    if len(backup_list) > 0:
        with pool_context(processes=appConfiguration.Threads) as p:
            p.map(backup, backup_list)

    if interrupted:
        app_exit(0)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Filename : log.py
    Date: 25.03.2018 09:48
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
import sys
import logging
from logging import handlers, Formatter


class Log:
    def __init__(self, name, log_file, log_level, log_count, max_bytes, dry=False):
        self.logger = logging.getLogger(name)
        if log_level == 'CRITICAL':
            self.logger.setLevel(logging.CRITICAL)
        elif log_level == 'ERROR':
            self.logger.setLevel(logging.ERROR)
        elif log_level == 'WARNING':
            self.logger.setLevel(logging.WARNING)
        elif log_level == 'INFO':
            self.logger.setLevel(logging.INFO)
        elif log_level == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.NOTSET)
        if dry:
            self.handler = logging.StreamHandler(sys.stdout)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.handler = handlers.RotatingFileHandler(log_file, backupCount=log_count, maxBytes=max_bytes)
        log_format = Formatter('[%(asctime)s] [%(levelname)-8s] - %(message)s')
        self.handler.setFormatter(log_format)
        self.logger.addHandler(self.handler)

    def error(self, text, exc_info=False):
        self.logger.error(text, exc_info=exc_info)

    def info(self, text, exc_info=False):
        self.logger.info(text, exc_info=exc_info)

    def warning(self, text, exc_info=False):
        self.logger.warning(text, exc_info=exc_info)

    def debug(self, text, exc_info=False):
        self.logger.debug(text, exc_info=exc_info)

    def critical(self, text, exc_info=False):
        self.logger.critical(text, exc_info=exc_info)

    def __del__(self):
        self.handler.close()
        self.logger.removeHandler(self.handler)
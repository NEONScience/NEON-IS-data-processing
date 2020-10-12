#!/usr/bin/env python3
import os
import json
from pathlib import Path
from typing import Iterator, BinaryIO

import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

from message_reader.message_reader_config import Config
from message_reader.message import Message
from message_reader.message_reader import run
import message_reader.message_reader_main as message_reader_main


class MessageReaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_file(self.out_path)
        self.bootstrap_server = '10.206.27.129:30937'
        self.topic = 'debezium_test.pdr.customers'
        self.group_id = 'my-group'
        self.auto_offset_reset = 'earliest'
        self.enable_auto_commit = True
        self.log_level = 'DEBUG'

    @unittest.skip('Integration test skipped.')
    def test_main(self):
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = self.log_level
        os.environ['BOOTSTRAP_SERVER'] = self.bootstrap_server
        os.environ['TOPIC'] = self.topic
        os.environ['GROUP_ID'] = self.group_id
        os.environ['AUTO_OFFSET_RESET'] = self.auto_offset_reset
        os.environ['ENABLE_AUTO_COMMIT'] = str(self.enable_auto_commit)
        message_reader_main.main()
        file_path = Path(self.out_path, '1')
        self.assertTrue(file_path.exists())

    def test_message_reader(self):

        def open_pipe(out_path: Path) -> BinaryIO:
            """
            Mock function for opening pipe to write files.

            :param out_path: The output directory for writing files.
            :return: The binary pipe for writing.
            """
            open_file = os.open(out_path, os.O_WRONLY)
            output_pipe = os.fdopen(open_file, 'wb')
            return output_pipe

        def read_messages() -> Iterator[Message]:
            """
            Mock function to return a message.

            :return: The message.
            """
            key = json.loads('{ "payload": { "id": "1" }}')
            value = json.loads('{ "test": "testing" }')
            return [Message(key=key, value=value)]

        config = Config(out_path=self.out_path,
                        bootstrap_server=self.bootstrap_server,
                        topic=self.topic,
                        group_id=self.group_id,
                        auto_offset_reset=self.auto_offset_reset,
                        enable_auto_commit=self.enable_auto_commit,
                        is_test=True)
        run(config, open_pipe, read_messages)
        self.assertTrue(self.out_path.exists())

#!/usr/bin/env python3
import os
from pathlib import Path
from typing import Iterator, BinaryIO

import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

from metadata_reader.metadata_reader_config import Config
from metadata_reader.message import Message
from metadata_reader.metadata_reader import MetadataReader
import metadata_reader.metadata_reader_main as metadata_reader_main


class MetadataReaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/pfs/out')
        self.fs.create_file(self.out_path)
        self.bootstrap_server = '10.206.27.129:30937'
        self.topic = 'debezium_test.pdr.customers'
        self.group_id = 'my-group'
        self.encoding = 'utf-8'
        self.auto_offset_reset = 'earliest'
        self.enable_auto_commit = True
        self.log_level = 'DEBUG'
        self.test_mode = True
        self.config = Config(out_path=self.out_path,
                             bootstrap_server=self.bootstrap_server,
                             topic=self.topic,
                             group_id=self.group_id,
                             encoding=self.encoding,
                             auto_offset_reset=self.auto_offset_reset,
                             enable_auto_commit=self.enable_auto_commit,
                             test_mode=True)

    @unittest.skip('Test skipped.')
    def test_main(self):
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = self.log_level
        os.environ['BOOTSTRAP_SERVER'] = self.bootstrap_server
        os.environ['TOPIC'] = self.topic
        os.environ['GROUP_ID'] = self.group_id
        os.environ['ENCODING'] = self.encoding
        os.environ['AUTO_OFFSET_RESET'] = self.auto_offset_reset
        os.environ['ENABLE_AUTO_COMMIT'] = str(self.enable_auto_commit)
        os.environ['TEST_MODE'] = str(self.test_mode)
        metadata_reader_main.main()
        file_path = Path(self.out_path, 'test')
        self.assertTrue(file_path.exists())

    def test_metadata_reader(self):

        def open_pipe(out_path: Path) -> BinaryIO:
            """
            Mock function for testing.

            :param out_path: The output directory for writing files.
            :return: The binary pipe for writing.
            """
            open_file = os.open(out_path, os.O_WRONLY)
            output_pipe = os.fdopen(open_file, 'wb')
            return output_pipe

        def read_messages(config: Config) -> Iterator[Message]:
            """
            Mock function to return a message.

            :param config: The Configuration
            :return: The message.
            """
            key = '1'
            content = '{ "test": "testing" }'
            return [Message(key=key, content=content)]

        # test
        metadata_reader = MetadataReader(self.config)
        metadata_reader.read(open_pipe=open_pipe, read_messages=read_messages)
        # check output
        self.assertTrue(self.out_path.exists())

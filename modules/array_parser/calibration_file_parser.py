#!/usr/bin/env python3
import xml.etree.ElementTree as ElementTree
from pathlib import Path


def get_stream_id(path: Path) -> str:
    """
    Parse the stream ID from the calibration file.

    :param path: The file path.
    :return: The stream ID.
    """
    xml_root = ElementTree.parse(path).getroot()
    calibration_stream = xml_root.find('StreamCalVal')
    stream_id = calibration_stream.find('StreamID').text
    return stream_id

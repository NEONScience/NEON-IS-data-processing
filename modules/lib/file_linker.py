#!/usr/bin/env python3
import os


def link(source, target):
    """
    Link the source into the target.

    :param source: The source path.
    :type source: str
    :param target: The target path.
    :type target: str
    :return:
    """
    if not os.path.exists(target):
        try:
            if os.path.isdir(target):
                os.makedirs(target)
            else:
                os.makedirs(os.path.dirname(target))
        except FileExistsError:
            pass
    try:
        os.symlink(source, target)
    except FileExistsError:
        os.remove(target)
        os.symlink(source, target)

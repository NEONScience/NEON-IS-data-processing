#!/usr/bin/env python3
import os


def link(source, target):
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

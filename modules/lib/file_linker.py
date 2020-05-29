#!/usr/bin/env python3
from pathlib import Path


def link(path: Path, link_path: Path):
    """
    Symbolically link the link path to the path.
    :param path:
    :param link_path:
    """
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        link_path.symlink_to(path)


# def link(source, target):
#     """
#     Link source to target.
#
#     :param source: The source path.
#     :type source: str
#     :param target: The target path.
#     :type target: str
#     :return:
#     """
#     if not os.path.exists(target):
#         try:
#             if os.path.isdir(target):
#                 os.makedirs(target)
#             else:
#                 os.makedirs(os.path.dirname(target))
#         except FileExistsError:
#             pass
#     try:
#         os.symlink(source, target)
#     except FileExistsError:
#         os.remove(target)
#         os.symlink(source, target)

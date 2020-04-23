#!/usr/bin/env python3
import os
import sys
import pathlib

from structlog import get_logger
import lib.file_linker as file_linker

log = get_logger()


class Egress(object):

    def __init__(self, dataPath, outPath, outputName, dateIndex, locIndex):
        """
        Constructor.

        :param dataPath: The data path.
        :type dataPath: str
        :param outPath: The output path for writing results.
        :type outPath: str
        :param outputName: The output name.
        :type outputName: str
        :param dateIndex: The date index.
        :type dateIndex: int
        :param locIndex: The location index.
        :type locIndex: int
        """
        self.outputName = outputName
        self.dataPath = dataPath
        self.outPath = outPath
        # date and loc indices refer to locations within the filename (not the path)
        self.dateIndex = dateIndex
        self.locIndex = locIndex
        self.filenameDelimiter = "_"

    def upload(self):
        """
        Link the source files into the output directory.

        :return:
        """
        try:
            for root, dirs, files in os.walk(self.dataPath):
                for filename in files:
                    if not filename.startswith('.'):

                        sourcePath = os.path.join(root, filename)
                        parts = pathlib.Path(sourcePath).parts
                        
                        # date
                        filenameParts = filename.split(self.filenameDelimiter)
                        dateTime = filenameParts[self.dateIndex]
                        # loc
                        loc = filenameParts[self.locIndex]

                        # construct target filename
                        targetParts = [self.outPath, self.outputName, dateTime, loc, filenameParts[len(filenameParts)-2], filenameParts[len(filenameParts)-1]]
                        targetFilename = self.filenameDelimiter.join(targetParts[1:])
                        targetPath = os.path.join(*targetParts[:len(targetParts)-2], targetFilename)

                        # symlink to target
                        print("sourcepath = " + sourcePath)
                        print("targetpath = " + targetPath)
                        file_linker.link(sourcePath, targetPath)

        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.error("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))

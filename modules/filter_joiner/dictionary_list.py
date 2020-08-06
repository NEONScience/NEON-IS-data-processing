#!/usr/bin/env python3


class DictionaryList(dict):
    """Class to automatically add all values with the same key into a list."""

    def __setitem__(self, key, value) -> None:
        try:
            # assumes a list exists on the key
            self[key].append(value)
        except KeyError:  # there is no key
            super(DictionaryList, self).__setitem__(key, value)
        except AttributeError:  # it is not a list
            super(DictionaryList, self).__setitem__(key, [self[key], value])

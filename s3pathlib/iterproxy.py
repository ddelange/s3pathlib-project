# -*- coding: utf-8 -*-

"""
Improve iter_objects API by giving it better iterator that support filters.
"""

from itertools import islice
from typing import Iterable, Iterator, Union, Set


class IterProxy:
    """
    An iterator proxy utility class provide client side in-memory filter.
    It is highly inspired by sqlalchemy Result Proxy that depends on SQL server
    side filter.

    Features:

    - :meth:`filter`:
    - :meth:`one`: take one item
    - :meth:`one_or_none`: take one item
    - :meth:`many`: take many item
    - :meth:`all`: take all item
    """

    def __init__(self, iterable: Iterable):
        self._iterable: Iterable = iterable
        self._iterator: Union[Iterator, None] = None
        self._filters: Union[list, tuple] = list()
        self._filters_set: Set[callable] = set()
        self._is_frozen: bool = False

    def _to_iterator(self):
        """
        Once the IterProxy becomes an iterator, don't allow
        adding / removing filters anymore.
        """
        if not self._is_frozen:
            # print("_to_iterator() is called! convert to iterator")
            self._iterator = iter(self._iterable)
            self._filters = tuple(self._filters)
            self._is_frozen = True

    def __iter__(self):
        self._to_iterator()
        return self

    def __next__(self):
        while 1:
            try:
                item = next(self._iterator)
            except StopIteration as e:
                raise e

            and_all_true = True
            for f in self._filters:
                if not f(item):
                    and_all_true = False
                    break

            if and_all_true:
                return item

    def filter(self, *funcs: callable):
        """

        >>> def is_odd(i):
        ...     return i %%

        >>> proxy = IterProxy(range(10)).filter(is_odd)

        :param funcs:
        :return:
        """
        for func in funcs:
            if func not in self._filters_set:
                try:
                    self._filters.append(func)
                except AttributeError:
                    raise PermissionError("you cannot update filters once iteration started!")
                self._filters_set.add(func)
        return self

    def one(self):
        """
        Return one item from the iterator.

        See also:

        - :meth:`many`
        - :meth:`all`
        """
        self._to_iterator()
        return next(self)

    def one_or_none(self):
        """
        Return one item from the iterator. If nothing left in the iterator,
        it returns None.
        """
        self._to_iterator()
        try:
            return next(self)
        except StopIteration:
            return None

    def many(self, k: int) -> list:
        """
        Return k item yield from iterator as a list.
        """
        l = list(islice(self, k))
        if len(l) == 0:
            raise StopIteration
        return l

    def all(self) -> list:
        """
        Return all remaining item in the iterator as a list.
        """
        self._to_iterator()
        return list(self)

    def skip(self, k: int):
        """
        Skip next k items.
        """
        self._to_iterator()
        for _ in islice(self, k):
            pass
        return self

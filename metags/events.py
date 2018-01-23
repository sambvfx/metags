"""
Simple in-process event system.

TODO: upgrade this event system
"""
import functools
from kids.cache import undecorate, SUPPORTED_DECORATOR

from typing import Callable, Union


# Cache of events being listened for. This is populated by the `listen`
# decorator.
_events = {}


def event(name_or_func):
    """
    Decorator for easily sending the results of the decorated function to any
    listeners.
    
    Parameters
    ----------
    name_or_func : Union[Callable, str]

    Returns
    -------
    Callable
    """
    def decorator(func):

        wrapper, wrapped = undecorate(func)

        @functools.wraps(wrapped)
        def wrap(*args, **kwargs):
            result = wrapped(*args, **kwargs)

            for e in _events.get(eventname, []):
                e(result)

            return result

        return wrapper(wrap)

    if (callable(name_or_func) or
            isinstance(name_or_func, tuple(SUPPORTED_DECORATOR.keys()))):
        eventname = None
        return decorator(name_or_func)
    else:
        eventname = name_or_func
        return lambda f: decorator(f)


def listen(name, highlander=False, singleton=False):
    """
    Decorator for easily listening for events by name.

    Parameters
    ----------
    name : str
    highlander : bool
        There can only be one. The last one to be called will be victorious.
        If a non-highlander listener is attempted to be added after a 
        highlander has been registered, an exception will be raised.
    singleton: bool
        Removes itself from the listeners after it fires once.

    Returns
    -------
    Callable
    """
    def decorator(func):

        listeners = _events.get(name, [])
        if highlander:
            listeners = (func,)
        else:
            assert not isinstance(listeners, tuple), \
                'A listener has already claimed this event {}'.format(name)
            listeners.append(func)
        _events[name] = listeners

        wrapper, wrapped = undecorate(func)

        @functools.wraps(wrapped)
        def wrap(*args, **kwargs):
            result = wrapped(*args, **kwargs)
            if singleton:
                _events[name].remove(func)
            return result

        return wrapper(wrap)
    return decorator

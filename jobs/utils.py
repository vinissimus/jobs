import typing


def resolve_dotted_name(name: str) -> typing.Any:
    """
    import the provided dotted name
    >>> resolve_dotted_name('guillotina.interfaces.IRequest')
    <InterfaceClass guillotina.interfaces.IRequest>
    :param name: dotted name
    """
    if not isinstance(name, str):
        return name  # already an object
    names = name.split(".")
    used = names.pop(0)
    found = __import__(used)
    for n in names:
        used += "." + n
        try:
            found = getattr(found, n)
        except AttributeError:
            __import__(used)
            found = getattr(found, n)

    return found

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("euler-onebot")
except PackageNotFoundError:
    __version__ = "unknown"

NAME = "Lagrange.OneBot"
VERSION = __version__

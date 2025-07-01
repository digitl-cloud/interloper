"""This module contains the custom errors for the interloper package."""


class InterloperError(Exception):
    """Base class for all interloper errors."""

    pass


#######################
# Source Errors
#######################


class SourceError(InterloperError):
    """Base class for source errors."""

    ...


class SourceDefinitionError(SourceError):
    """Raised when there is an error in a source definition."""

    ...


class SourceValueError(SourceError):
    """Raised when there is an error with a source value."""

    ...


class SourceParamError(SourceError):
    """Raised when there is an error with a source parameter."""

    ...


#######################
# Asset Errors
#######################


class AssetError(InterloperError):
    """Base class for asset errors."""

    ...


class AssetDefinitionError(AssetError):
    """Raised when there is an error in an asset definition."""

    ...


class AssetValueError(AssetError):
    """Raised when there is an error with an asset value."""

    ...


class AssetNormalizationError(AssetError):
    """Raised when there is an error normalizing an asset."""

    ...


class AssetMaterializationError(AssetError):
    """Raised when there is an error materializing an asset."""

    ...


class AssetSchemaError(AssetError):
    """Raised when there is an error with an asset schema."""

    ...


#######################
# Asset Param Errors
#######################


class AssetParamError(InterloperError):
    """Base class for asset parameter errors."""

    ...


class AssetParamResolutionError(AssetParamError):
    """Raised when there is an error resolving an asset parameter."""

    ...


class UpstreamAssetError(AssetParamError):
    """Raised when there is an error with an upstream asset."""

    ...

class InterloperError(Exception):
    pass


#######################
# Source Errors
#######################


class SourceError(InterloperError): ...


class SourceDefinitionError(SourceError): ...


class SourceValueError(SourceError): ...


class SourceParamError(SourceError): ...


#######################
# Asset Errors
#######################


class AssetError(InterloperError): ...


class AssetDefinitionError(AssetError): ...


class AssetValueError(AssetError): ...


class AssetNormalizationError(AssetError): ...


class AssetMaterializationError(AssetError): ...


class AssetSchemaError(AssetError): ...


#######################
# Asset Param Errors
#######################


class AssetParamError(InterloperError): ...


class AssetParamResolutionError(AssetParamError): ...


class UpstreamAssetError(AssetParamError): ...

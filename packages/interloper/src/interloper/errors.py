class InterloperError(Exception):
    pass


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

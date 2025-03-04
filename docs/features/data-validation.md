## Asset's data type

```py
@asset
def my_asset() -> str:
    return 123
```

```
TypeError: Asset my_asset_A returned data of type int, expected str
```

## Upstream assets's data type

```py
@source
def my_source():
    @asset(name="A")
    def my_asset_A() -> str:
        return "A"

    @asset(name="B")
    def my_asset_B(
        a = UpstreamAsset("A", type=int),
    ) -> str:
        return "B"
```

```
TypeError: Expected data of type int from upstream asset B, but got str
```

## `TypedIO` data materialization

!!! danger "TODO"
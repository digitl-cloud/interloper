## Functional definition
```py
@source
def my_source():
    @asset
    def my_asset_A():
        return "A"

    @asset
    def my_asset_B():
        return "B"

    return (my_asset_A, my_asset_B)
```

## Object oriented definition

```py
class MyAssetA(Asset):
    def data(self):
        return "A"

class MyAssetB(Asset):
    def data(self):
        return "B"

class MySource(Source):
    def asset_definitions(self):
        return (
            MyAssetA("my_asset_A"),
            MyAssetB("my_asset_B"),
        )

my_source = MySource("my_source")
```

## Parameters

```py
@asset
def my_asset(who: str = "world"):
    return f"hello {world}"
```


## Running an asset
```py
my_asset.run(who="you")
```

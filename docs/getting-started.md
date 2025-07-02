```sh
pip install interloper
```

```py
@source
def my_source() -> Sequence[Asset]:
    @asset
    def my_asset_A() -> str:
        return "A"

    @asset
    def my_asset_B() -> str:
        return "B"

    return (my_asset_A, my_asset_B)

my_source.io ={"file": interloper.io.FileIO("./data")}

Pipeline.materialize(my_source)

```
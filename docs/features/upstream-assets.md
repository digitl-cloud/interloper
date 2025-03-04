```py
@source
def my_source():
    @asset(name="A")
    def my_asset_A():
        return "A"

    @asset(name="B")
    def my_asset_B(
        a: str = UpstreamAsset("A"),
    ):
        return "B"

    @asset(name="C")
    def my_asset_C(
        a: str = UpstreamAsset("custom_ref_A"),
        b: str = UpstreamAsset("custom_ref_B"),
    ):
        return "C"

    return (my_asset_A, my_asset_B, my_asset_C)


my_source.io = {"file": FileIO("data")}

my_source.C.deps = {
    "custom_ref_A": "A",
    "custom_ref_B": "B",
}

Pipeline(my_source).materialize()
```

!!! note
    Note how the upstream assets's refs of `my_asset_C` do not match the name of their corresponding asset (e.g. `custom_ref_A` ≠ `A`).
    Therefore, the `deps` config of `my_asset_C` has to be defined manually, mapping an upstream key to its corresponding asset.
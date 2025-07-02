
```py
@source
def my_source():
    @asset
    def my_asset():
        return "hello"

    return (my_asset_A,)
```

## Simple IO

```py
my_source = my_source(io={"file": FileIO("./data")})

Pipeline(my_source).materialize()
```


## Multiple IO

```py
my_source = my_source(
    io={
        "duckdb": DuckDBDataframeIO("data/duck.db"),
        "sqlite": SQLiteDataframeIO("data/sqlite.db"),
    },
    default_io_key = "duckdb",
)

Pipeline(my_source).materialize()
```
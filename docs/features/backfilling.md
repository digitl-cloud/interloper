## Multi run

```py
@source
def my_source() -> Sequence[Asset]:
    @asset(
        partitioning=TimePartitionConfig(column="date"),
    )
    def my_asset(
        date: dt.date = Date(),
    ) -> str:
        return "hello"

    return (my_asset,)


my_source.io = {"file": FileIO("data")}

Pipeline(my_source).backfill(
    partitions=TimePartitionWindow(
        start=dt.date.today() - dt.timedelta(days=3),
        end=dt.date.today() - dt.timedelta(days=1),
    )
)

```

## Single run
```py
@source
def my_source() -> Sequence[Asset]:
    @asset(
        partitioning=TimePartitionConfig(column="date", allow_window=True),
    )
    def my_asset(
        date_window: tuple[dt.date, dt.date] = DateWindow(),
    ) -> str:
        return "hello"

    return (my_asset,)


my_source.io = {"file": FileIO("data")}

Pipeline(my_source).backfill(
    partitions=TimePartitionWindow(
        start=dt.date.today() - dt.timedelta(days=3),
        end=dt.date.today() - dt.timedelta(days=1),
    )
)
```
## Time partitioning

```py
@source
def my_source() -> Sequence[Asset]:
    @asset(
        partitioning=TimePartitionConfig(column="date"),
    )
    def my_asset_A(
        date: dt.date = Date(),
    ) -> str:
        return "A"

    @asset(
        partitioning=TimePartitionConfig(column="date", allow_window=True),
    )
    def my_asset_B(
        date_window: tuple[dt.date, dt.date] = DateWindow(),
    ) -> str:
        return "B"

    return (my_asset_A,)


my_source.io = {"file": FileIO("data")}

Pipeline(my_source).materialize(partition=TimePartition(dt.date.today()))

```
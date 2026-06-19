
import interloper as il


@il.source(
    tags=["Analytics"],
)
class CampaignPerformanceAnalysis(il.Source):
    """Demo source. Defines a small DAG (a -> b,c,d -> e) with time partitioning."""

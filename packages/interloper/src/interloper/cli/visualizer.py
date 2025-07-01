"""This module contains the MaterializationVisualizer class."""
import functools

from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from interloper.asset.base import Asset
from interloper.execution.execution import TExecutionStateBySource, TPartition
from interloper.execution.state import ExecutionState, ExecutionStatus
from interloper.partitioning.window import PartitionWindow


# TODO: ensure partitions are sorted
class MaterializationVisualizer:
    """A class to visualize the materialization of assets."""

    def render_progress(self, state: TExecutionStateBySource) -> Panel:
        """Render the asset progress as a panel of grouped Rich Trees.

        Args:
            state: The execution state.

        Returns:
            A panel with the progress.
        """
        elements = []

        max_asset_name_length = max(max(len(asset.name) for asset in states.keys()) for states in state.values())
        max_partition_count = max(
            max(len(partitions.values()) for partitions in states.values()) for states in state.values()
        )

        for source, state_by_asset in state.items():
            label = "<no source>" if source is None else source.name
            tree = Tree(Text(label, style="bold cyan"), guide_style="dim")
            for asset, state_by_partition in state_by_asset.items():
                line = self._render_asset_line(
                    asset,
                    state_by_partition,
                    max_asset_name_length,
                    max_partition_count,
                )
                tree.add(line)
            elements.append(tree)

        return Panel(
            Group(*elements),
            title="Materialization Progress",
            border_style="cyan",
            padding=(0, 1),
            expand=False,
        )

    @functools.cache
    def _get_max_asset_name_length(self, state: TExecutionStateBySource) -> int:
        return max(max(len(asset.name) for asset in states.keys()) for states in state.values())

    @functools.cache
    def _get_max_partition_count(self, state: TExecutionStateBySource) -> int:
        return max(max(len(partitions.values()) for partitions in states.values()) for states in state.values())

    def _render_asset_line(
        self,
        asset: Asset,
        state_by_partition: dict[TPartition, ExecutionState],
        max_asset_name_length: int,
        max_partition_count: int,
    ) -> Table:
        table = Table(show_header=False, box=None, show_lines=False, pad_edge=False, expand=False)
        table.add_column("Name", min_width=max_asset_name_length)
        table.add_column("Status", min_width=1)
        table.add_column("Progress", min_width=max_partition_count)
        table.add_column("Percent", min_width=5, justify="right", style="magenta")

        completed = 0
        failed = 0
        has_running = False

        progress = Text()
        for partition, state in state_by_partition.items():
            if partition is None:
                char = "●"
            elif isinstance(partition, PartitionWindow):
                char = "━"
            else:
                char = "▪"

            if state.status == ExecutionStatus.SUCCESSFUL:
                progress.append(char, style="bold green")
                completed += 1
            elif state.status == ExecutionStatus.FAILED:
                progress.append(char, style="bold red")
                completed += 1
                failed += 1
            elif state.status == ExecutionStatus.BLOCKED:
                progress.append(char, style="dim red")
                completed += 1
                failed += 1
            elif state.status == ExecutionStatus.RUNNING:
                progress.append(char, style="yellow")
                has_running = True
            else:
                progress.append(char, style="dim")

        total = len(state_by_partition)
        pct = int((completed / total) * 100)

        spinner = Spinner("dots") if has_running else ""
        # name_style = "bold white" if has_running or completed != total else "bold red" if failed > 0 else "bold green"
        table.add_row(asset.name, spinner, progress, f"{pct}%")
        return table

    def render_failure_summary(self, errors: list[Exception]) -> Panel:
        """Render the failure summary as a panel.

        Args:
            errors: The list of errors.

        Returns:
            A panel with the failure summary.
        """
        table = Table(
            show_header=False,
            box=None,
            show_lines=False,
            pad_edge=False,
            expand=False,
        )
        table.add_column("Partition", style="dim", no_wrap=True)
        table.add_column("Asset", style="bold white", no_wrap=True)
        table.add_column("Error", style="red")

        for error in errors:
            table.add_row("xx", "xx", str(error))

        return Panel(
            table,
            title="Failure Summary",
            border_style="red",
            padding=(0, 1),
            expand=False,
        )

    def render_all(self, state: TExecutionStateBySource, errors: list[Exception]) -> RenderableType:
        """Render the progress and failure summary.

        Args:
            state: The execution state.
            errors: The list of errors.

        Returns:
            A renderable object.
        """
        if len(errors) > 0:
            return Group(
                self.render_progress(state),
                self.render_failure_summary(errors),
            )
        else:
            return Group(self.render_progress(state))

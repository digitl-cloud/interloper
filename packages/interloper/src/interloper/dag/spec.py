"""This module contains the DAG specification."""

from pydantic import BaseModel

from interloper.asset.spec import AssetSpec
from interloper.dag.base import DAG
from interloper.io.spec import IOSpec
from interloper.source.spec import SourceSpec


class DAGSpec(BaseModel):
    """A specification for a DAG."""

    io: dict[str, IOSpec]
    assets: list[SourceSpec | AssetSpec]

    def to_dag(self) -> DAG:
        """Convert the specification to a DAG.

        Returns:
            The DAG.
        """
        io = {name: IOSpec.model_validate(spec).to_io() for name, spec in self.io.items()}

        sources_or_assets = []
        for spec in self.assets:
            if spec.type == "asset":
                asset = AssetSpec.model_validate(spec).to_asset()
                asset.io = asset.io or io
                sources_or_assets.append(asset)
            elif spec.type == "source":
                source = SourceSpec.model_validate(spec).to_source()
                source.io = source.io or io
                sources_or_assets.append(source)

        dag = DAG()
        for asset_or_source in sources_or_assets:
            dag.add_assets(asset_or_source)
        return dag

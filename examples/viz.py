from collections.abc import Sequence
from typing import Any

from interloper.core.asset import Asset, asset
from interloper.core.pipeline import Pipeline
from interloper.core.param import UpstreamAsset
from interloper.core.source import source
import matplotlib.pyplot as plt
import networkx as nx
import streamlit as st


@source
def X() -> Sequence[Asset]:
    @asset
    def A() -> Any:
        return "A"

    @asset
    def B(
        a: Any = UpstreamAsset("A"),
    ) -> Any:
        return "B"

    return (A, B)


pipeline = Pipeline(X)


fig, ax = plt.subplots()

G = nx.convert_node_labels_to_integers(pipeline.graph)
G = nx.relabel_nodes(
    G, {index: node.name for index, node in enumerate(pipeline.graph.nodes)}
)
pos = nx.planar_layout(G)
nx.draw(G, pos, with_labels=True)

st.pyplot(fig)

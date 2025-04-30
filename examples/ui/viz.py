from collections.abc import Sequence
from typing import Any

import interloper as itlp
import matplotlib.pyplot as plt
import networkx as nx
import streamlit as st


@itlp.source
def X() -> Sequence[itlp.Asset]:
    @itlp.asset
    def A() -> Any:
        return "A"

    @itlp.asset
    def B(
        a: Any = itlp.UpstreamAsset("A"),
    ) -> Any:
        return "B"

    @itlp.asset
    def C(
        a: Any = itlp.UpstreamAsset("A"),
        b: Any = itlp.UpstreamAsset("B"),
    ) -> Any:
        return "C"

    @itlp.asset
    def D(
        a: Any = itlp.UpstreamAsset("A"),
    ) -> Any:
        return "D"

    return (A, B, C, D)


pipeline = itlp.Pipeline(X)


fig, ax = plt.subplots()

G = nx.convert_node_labels_to_integers(pipeline._graph)
G = nx.relabel_nodes(
    G, {index: node.name for index, node in enumerate(pipeline._graph.nodes)}
)
pos = nx.planar_layout(G)
nx.draw(G, pos, with_labels=True)

st.pyplot(fig)

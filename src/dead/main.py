import datetime as dt
from collections.abc import Sequence
from typing import Any

from dead.assets.adservice import adservice
from dead.core.asset import Asset, asset
from dead.core.io import FileIO
from dead.core.pipeline import Pipeline
from dead.core.sentinel import Env, UpstreamAsset
from dead.core.source import source

##################
# OOP
##################
# class AssetX(Asset):
#     def data(self) -> Any:
#         return ["x1", "x2"]


# class AssetY(Asset):
#     def data(
#         self,
#         x: Any = UpstreamAsset("X"),
#     ) -> Any:
#         return ["y1", "y2"]


# class SourceZ(Source):
#     def asset_definitions(self) -> Sequence[Asset]:
#         return AssetX("X"), AssetY("Y")
""

# z = SourceZ("Z")
# z.io = FileIO("/Users/g/Downloads/dead")
# Pipeline(z).materialize()


##################
# FUNCTIONAL
##################


# @source
# def Z(
#     key: str = Env("KEY_A"),
# ) -> Sequence[Asset]:
#     @asset
#     def W() -> Any:
#         return ["w1", "w2"]

#     @asset
#     def X(
#         w: Any = UpstreamAsset("W"),
#     ) -> Any:
#         return ["x1", "x2"]

#     @asset
#     def Y(
#         w: Any = UpstreamAsset("W"),
#         x: Any = UpstreamAsset("X"),
#     ) -> Any:
#         return ["y1", "y2"]

#     return (W, X, Y)


# Z.io = {
#     "file": FileIO("/Users/g/Downloads/dead"),
#     "file2": FileIO("/Users/g/Downloads/dead2"),
# }
# Z.default_io_key = "file"

# pipeline = Pipeline(Z)
# pipeline.materialize()


##################
# VISUALIZATION
##################

# import matplotlib.pyplot as plt
# import networkx as nx
# import streamlit as st

# fig, ax = plt.subplots()

# G = nx.convert_node_labels_to_integers(pipeline.graph)
# G = nx.relabel_nodes(G, {index: node.name for index, node in enumerate(pipeline.graph.nodes)})
# pos = nx.planar_layout(G)
# nx.draw(G, pos, with_labels=True)

# st.pyplot(fig)


adservice.io = {
    "file": FileIO("/Users/g/Downloads/dead"),
}
adservice.campaigns.bind(date=dt.date(2024, 1, 1))

pipeline = Pipeline(adservice)
pipeline.materialize()

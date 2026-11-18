from interloper_pandas import DataFrameNormalizer


class CampaignManager360Normalizer(DataFrameNormalizer):
    """Snake-case CM360's CSV report headers onto the schema columns.

    Headers are human-readable ("Active View: % Viewable Impressions",
    "Site (CM360)", "DV360 Cost (USD)"). The generic pass drops ``%`` outright,
    which would collide "% Viewable Impressions" with "Viewable Impressions", so
    spell it out as ``pct`` first, then defer to snake-casing with digit
    splitting (CM360 → cm_360).
    """

    def column_name(self, name: str) -> str:
        """Rewrite one vendor column name before the standard mapping.

        Args:
            name: The column name as the API returned it.

        Returns:
            The mapped column name.
        """
        return super().column_name(name.replace("%", "pct"))

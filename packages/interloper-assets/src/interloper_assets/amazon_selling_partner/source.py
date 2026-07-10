
import interloper as il

from interloper_assets.amazon_selling_partner.connection import AmazonSellingPartnerConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    resources={"connection": AmazonSellingPartnerConnection},
    tags=["E-Commerce"],
    icon="icon:amazon",
)
class AmazonSellingPartner(il.Source):
    """Amazon Selling Partner integration for vendor and seller reporting."""

    marketplace: str = il.SelectField(
        options=[
            {"label": "United States", "value": "ATVPDKIKX0DER"},
            {"label": "Canada", "value": "A2EUQ1WTGCTBG2"},
            {"label": "Mexico", "value": "A1AM78C64UM0Y8"},
            {"label": "Brazil", "value": "A2Q3Y263D00KWC"},
            {"label": "United Kingdom", "value": "A1F83G8C2ARO7P"},
            {"label": "Germany", "value": "A1PA6795UKMFR9"},
            {"label": "France", "value": "A13V1IB3VIYZZH"},
            {"label": "Italy", "value": "APJ6JRA9NG5V4"},
            {"label": "Spain", "value": "A1RKKUPIHCS9HS"},
            {"label": "Netherlands", "value": "A1805IZSGTT6HS"},
            {"label": "Sweden", "value": "A2NODRKZP88ZB9"},
            {"label": "Poland", "value": "A1C3SOZRARQ6R3"},
            {"label": "Belgium", "value": "AMEN7PMS3EDWL"},
            {"label": "India", "value": "A21TJRUUN4KGV"},
            {"label": "Turkey", "value": "A33AVAJ2PDY3EV"},
            {"label": "Saudi Arabia", "value": "A17E79C6D8DWNP"},
            {"label": "United Arab Emirates", "value": "A2VIGQ35RCS4UG"},
            {"label": "Egypt", "value": "ARBP9OOSHTCHU"},
            {"label": "South Africa", "value": "AE08WJ6YKNBMC"},
            {"label": "Japan", "value": "A1VC38T7YXB528"},
            {"label": "Australia", "value": "A39IBJ37TRP1C6"},
            {"label": "Singapore", "value": "A19VAU5U5O7RUS"},
        ],
        description="Amazon marketplace",
        discriminator=True,
    )

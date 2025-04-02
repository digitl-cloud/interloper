from dataclasses import dataclass, field

import interloper as itlp


@dataclass
class ConversionsByTimeOfDay(itlp.AssetSchema):
    """
    The Conversions by Time of Day report provides insights into conversion patterns based on the day of the week
    and hour of the day. It includes key metrics such as the number of conversions and revenue generated within each
    time segment.
    """

    conversions: int = field(metadata={"description": "Number of conversions"})
    day: str = field(metadata={"description": "Day of the week"})
    hour: int = field(metadata={"description": "Hour of the day"})
    revenue: float = field(metadata={"description": "Revenue generated"})

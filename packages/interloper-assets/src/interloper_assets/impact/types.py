from enum import Enum


class ImpactCampaignStatus(Enum):
    """Lifecycle state of an Impact campaign."""
    ACTIVE = "ACTIVE"
    AWAITING_REVIEW = "AWAITING_REVIEW"
    CANCELLED = "CANCELLED"
    CLOSED = "CLOSED"
    DEACTIVATED = "DEACTIVATED"
    DELINQUENT = "DELINQUENT"
    REJECTED = "REJECTED"
    SETUP = "SETUP"
    SETUP_COMPLETE = "SETUP_COMPLETE"


class ImpactActionStatus(Enum):
    """Review state of a tracked action (conversion)."""
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REVERSED = "REVERSED"


class ImpactResolutionStatus(Enum):
    """How a disputed action was resolved."""
    UNRESOLVED = "UNRESOLVED"
    VALID = "VALID"
    DECLINED = "DECLINED"
    INCOMPLETE = "INCOMPLETE"


class ImpactAdType(Enum):
    """Creative format of an Impact ad."""
    BANNER = "BANNER"
    TEXT_LINK = "TEXT_LINK"
    COUPON = "COUPON"


class ImpactCatalogStatus(Enum):
    """Processing state of a product catalog."""
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    DEACTIVATED = "DEACTIVATED"
    PENDING = "PENDING"


class ImpactContractStatus(Enum):
    """Lifecycle state of a partner contract."""
    ACTIVE = "ACTIVE"
    DECLINED = "DECLINED"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"
    UPCOMING = "UPCOMING"


class ImpactDealScope(Enum):
    """Breadth a deal applies to."""
    PRODUCT = "PRODUCT"
    CATEGORY = "CATEGORY"
    ENTIRE_STORE = "ENTIRE_STORE"


class ImpactDealStatus(Enum):
    """Lifecycle state of a deal."""
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"


class ImpactDealType(Enum):
    """Kind of incentive a deal offers."""
    GENERAL_SALE = "GENERAL_SALE"
    FREE_SHIPPING = "FREE_SHIPPING"
    GIFT_WITH_PURCHASE = "GIFT_WITH_PURCHASE"
    REBATE = "REBATE"
    BOGO = "BOGO"


class ImpactExceptionListState(Enum):
    """Whether an exception list is in force."""
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    DEACTIVATED = "DEACTIVATED"


class ImpactExceptionListType(Enum):
    """What an exception list matches on."""
    CATEGORY = "CATEGORY"
    SKU = "SKU"


class ImpactPartnerStatus(Enum):
    """Relationship state of a partner (publisher)."""
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"
    DECLINED = "DECLINED"
    SUSPENDED = "SUSPENDED"
    DEACTIVATED = "DEACTIVATED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ImpactPromoCodeStatus(Enum):
    """Whether a promo code can still be redeemed."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class ImpactUniqueURLStatus(Enum):
    """Whether a unique tracking URL is still live."""
    AVAILABLE = "AVAILABLE"
    DELETED = "DELETED"
    INUSE = "INUSE"
    QUARANTINE = "QUARANTINE"
    RESERVED = "RESERVED"

from enum import Enum


class ImpactCampaignStatus(Enum):
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
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REVERSED = "REVERSED"


class ImpactResolutionStatus(Enum):
    UNRESOLVED = "UNRESOLVED"
    VALID = "VALID"
    DECLINED = "DECLINED"
    INCOMPLETE = "INCOMPLETE"


class ImpactAdType(Enum):
    BANNER = "BANNER"
    TEXT_LINK = "TEXT_LINK"
    COUPON = "COUPON"


class ImpactCatalogStatus(Enum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    DEACTIVATED = "DEACTIVATED"
    PENDING = "PENDING"


class ImpactContractStatus(Enum):
    ACTIVE = "ACTIVE"
    DECLINED = "DECLINED"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"
    UPCOMING = "UPCOMING"


class ImpactDealScope(Enum):
    PRODUCT = "PRODUCT"
    CATEGORY = "CATEGORY"
    ENTIRE_STORE = "ENTIRE_STORE"


class ImpactDealStatus(Enum):
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"


class ImpactDealType(Enum):
    GENERAL_SALE = "GENERAL_SALE"
    FREE_SHIPPING = "FREE_SHIPPING"
    GIFT_WITH_PURCHASE = "GIFT_WITH_PURCHASE"
    REBATE = "REBATE"
    BOGO = "BOGO"


class ImpactExceptionListState(Enum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    DEACTIVATED = "DEACTIVATED"


class ImpactExceptionListType(Enum):
    CATEGORY = "CATEGORY"
    SKU = "SKU"


class ImpactPartnerStatus(Enum):
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
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class ImpactUniqueURLStatus(Enum):
    AVAILABLE = "AVAILABLE"
    DELETED = "DELETED"
    INUSE = "INUSE"
    QUARANTINE = "QUARANTINE"
    RESERVED = "RESERVED"

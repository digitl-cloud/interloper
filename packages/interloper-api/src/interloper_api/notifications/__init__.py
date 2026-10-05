"""Outbound notifications the API sends on a user's behalf."""

from interloper_api.notifications.base import Email
from interloper_api.notifications.invitations import InvitationEmail
from interloper_api.notifications.super_admins import SuperAdminPromotionEmail

__all__ = ["Email", "InvitationEmail", "SuperAdminPromotionEmail"]

import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsConversionsStats(Schema):
    """Pinterest ad performance per ad and day with the full conversion breakdown: conversions by event, attribution (click, engagement, view), channel (web, in-app, offline) and device, with values, quantities, cost per action and ROAS."""

    date: dt.date | None = Field(default=None, description="The report day.")
    ad_account_id: str | None = Field(default=None, description="The ad account ID.")
    ad_group_bid_multiplier: float | None = Field(
        default=None, description="The bid multiplier applied to the ad group."
    )
    ad_group_budget_in_local_currency: float | None = Field(
        default=None, description="The ad group budget in the account's currency."
    )
    ad_group_budget_type: str | None = Field(default=None, description="The ad group budget type.")
    ad_group_entity_status: str | None = Field(default=None, description="The ad group entity status.")
    ad_group_id: str | None = Field(default=None, description="The ad group ID.")
    ad_group_name: str | None = Field(default=None, description="The ad group name.")
    ad_group_optimization: str | None = Field(default=None, description="The ad group optimization (conversion) type.")
    ad_group_status: str | None = Field(default=None, description="The ad group status.")
    ad_id: str | None = Field(default=None, description="The ad ID.")
    ad_name: str | None = Field(default=None, description="The ad name.")
    ad_status: str | None = Field(default=None, description="The ad status.")
    advertiser_id: str | None = Field(default=None, description="The advertiser (ad account) ID.")
    app_install_cost_per_action: float | None = Field(default=None, description="Cost per app install.")
    campaign_budget_optimization: str | None = Field(
        default=None, description="Whether the campaign uses campaign budget optimization."
    )
    campaign_daily_spend_cap: float | None = Field(default=None, description="The campaign's daily spend cap.")
    campaign_entity_status: str | None = Field(default=None, description="The campaign entity status.")
    campaign_id: str | None = Field(default=None, description="The campaign ID.")
    campaign_lifetime_spend_cap: float | None = Field(default=None, description="The campaign's lifetime spend cap.")
    campaign_managed_status: str | None = Field(default=None, description="The campaign managed status.")
    campaign_name: str | None = Field(default=None, description="The campaign name.")
    campaign_objective_type: str | None = Field(default=None, description="The campaign objective type.")
    campaign_status: str | None = Field(default=None, description="The campaign status.")
    checkout_roas: float | None = Field(default=None, description="Return on ad spend for checkouts.")
    clickthrough_1_gross: int | None = Field(
        default=None, description="Gross paid Pin clicks (before invalid-traffic filtering)."
    )
    clickthrough_1: int | None = Field(default=None, description="Paid Pin clicks.")
    clickthrough_2: int | None = Field(default=None, description="Earned Pin clicks.")
    cost_per_lead: float | None = Field(default=None, description="Cost per lead.")
    cost_per_outbound_click_in_dollar_1: float | None = Field(
        default=None, description="Cost per paid outbound click, in dollars."
    )
    cost_per_outbound_click_in_dollar: float | None = Field(
        default=None, description="Cost per outbound click, in dollars."
    )
    cpc_in_micro_dollar: float | None = Field(default=None, description="Cost per click in micro dollars.")
    cpcv_in_micro_dollar: float | None = Field(
        default=None, description="Cost per completed video view, in micro-dollars."
    )
    cpcv_p95_in_micro_dollar: float | None = Field(
        default=None, description="Cost per video view reaching 95% of its length, in micro-dollars."
    )
    cpm_in_dollar: float | None = Field(default=None, description="Cost per mille in dollars.")
    cpm_in_micro_dollar: float | None = Field(default=None, description="Cost per mille in micro dollars.")
    cpv_in_micro_dollar: float | None = Field(default=None, description="Cost per video view, in micro-dollars.")
    ctr_2: float | None = Field(default=None, description="Earned click-through rate.")
    ctr: float | None = Field(default=None, description="Click-through rate.")
    custom_roas: float | None = Field(default=None, description="Return on ad spend for custom conversions.")
    ecpc_in_dollar: float | None = Field(default=None, description="Effective cost per click in dollars.")
    ecpc_in_micro_dollar: float | None = Field(default=None, description="Effective cost per click in micro dollars.")
    ecpcv_in_dollar: float | None = Field(default=None, description="Effective cost per completed view in dollars.")
    ecpcv_p95_in_dollar: float | None = Field(
        default=None, description="Effective cost per video view reaching 95% of its length, in dollars."
    )
    ecpe_in_dollar: float | None = Field(default=None, description="Effective cost per engagement in dollars.")
    ecpm_in_micro_dollar: float | None = Field(default=None, description="Effective cost per mille in micro dollars.")
    ecpv_in_dollar: float | None = Field(default=None, description="Effective cost per view in dollars.")
    ectr: float | None = Field(default=None, description="Effective click-through rate.")
    eengagement_rate: float | None = Field(default=None, description="Effective engagement rate.")
    engagement_1: int | None = Field(default=None, description="Paid engagements.")
    engagement_2: int | None = Field(default=None, description="Earned engagements.")
    engagement_rate: float | None = Field(default=None, description="Overall engagement rate.")
    idea_pin_page_backward_1: int | None = Field(default=None, description="Paid backward page swipes on Idea Pins.")
    idea_pin_page_backward_2: int | None = Field(default=None, description="Earned backward page swipes on Idea Pins.")
    idea_pin_page_forward_1: int | None = Field(default=None, description="Paid forward page swipes on Idea Pins.")
    idea_pin_page_forward_2: int | None = Field(default=None, description="Earned forward page swipes on Idea Pins.")
    idea_pin_product_tag_visit_1: int | None = Field(default=None, description="Paid product tag visits on Idea Pins.")
    idea_pin_product_tag_visit_2: int | None = Field(
        default=None, description="Earned product tag visits on Idea Pins."
    )
    impression_1_gross: int | None = Field(
        default=None, description="Gross paid impressions (before invalid-traffic filtering)."
    )
    impression_2: int | None = Field(default=None, description="Earned impressions.")
    inapp_add_to_cart_cost_per_action: float | None = Field(
        default=None, description="Cost per in-app add-to-cart conversion."
    )
    inapp_add_to_cart_roas: float | None = Field(
        default=None, description="Return on ad spend for in-app add-to-cart conversions."
    )
    inapp_app_install_cost_per_action: float | None = Field(
        default=None, description="Cost per in-app app-install conversion."
    )
    inapp_app_install_roas: float | None = Field(
        default=None, description="Return on ad spend for in-app app-install conversions."
    )
    inapp_checkout_cost_per_action: float | None = Field(
        default=None, description="Cost per in-app checkout conversion."
    )
    inapp_checkout_roas: float | None = Field(
        default=None, description="Return on ad spend for in-app checkout conversions."
    )
    inapp_search_cost_per_action: float | None = Field(default=None, description="Cost per in-app search conversion.")
    inapp_search_roas: float | None = Field(
        default=None, description="Return on ad spend for in-app search conversions."
    )
    inapp_signup_cost_per_action: float | None = Field(default=None, description="Cost per in-app signup conversion.")
    inapp_signup_roas: float | None = Field(
        default=None, description="Return on ad spend for in-app signup conversions."
    )
    inapp_unknown_cost_per_action: float | None = Field(default=None, description="Cost per in-app unknown conversion.")
    inapp_unknown_roas: float | None = Field(
        default=None, description="Return on ad spend for in-app unknown conversions."
    )
    is_premiere_campaign: str | None = Field(
        default=None, description="Whether the campaign is a Premiere Spotlight campaign."
    )
    leads: int | None = Field(default=None, description="Leads collected through lead ads.")
    offline_checkout_cost_per_action: float | None = Field(
        default=None, description="Cost per offline checkout conversion."
    )
    offline_checkout_roas: float | None = Field(
        default=None, description="Return on ad spend for offline checkout conversions."
    )
    offline_custom_cost_per_action: float | None = Field(
        default=None, description="Cost per offline custom conversion."
    )
    offline_custom_roas: float | None = Field(
        default=None, description="Return on ad spend for offline custom conversions."
    )
    offline_lead_cost_per_action: float | None = Field(default=None, description="Cost per offline lead conversion.")
    offline_lead_roas: float | None = Field(
        default=None, description="Return on ad spend for offline lead conversions."
    )
    offline_signup_cost_per_action: float | None = Field(
        default=None, description="Cost per offline signup conversion."
    )
    offline_signup_roas: float | None = Field(
        default=None, description="Return on ad spend for offline signup conversions."
    )
    offline_unknown_cost_per_action: float | None = Field(
        default=None, description="Cost per offline unknown conversion."
    )
    offline_unknown_roas: float | None = Field(
        default=None, description="Return on ad spend for offline unknown conversions."
    )
    onsite_checkouts_1: int | None = Field(default=None, description="Paid on-site (Pinterest) checkouts.")
    order_line_id: str | None = Field(default=None, description="The order line ID.")
    order_line_name: str | None = Field(default=None, description="The order line name.")
    outbound_click_1: int | None = Field(default=None, description="Paid outbound clicks.")
    outbound_click_2: int | None = Field(default=None, description="Earned outbound clicks.")
    outbound_ctr_1: float | None = Field(default=None, description="Paid outbound click-through rate.")
    page_visit_cost_per_action: float | None = Field(default=None, description="Cost per page visit.")
    page_visit_roas: float | None = Field(default=None, description="Return on ad spend for page visits.")
    paid_impression: int | None = Field(default=None, description="Paid impressions.")
    paid_video_viewable_rate: float | None = Field(
        default=None, description="Share of paid video impressions that were viewable."
    )
    pin_id: str | None = Field(default=None, description="The ID of the organic Pin behind the ad.")
    pin_promotion_id: str | None = Field(default=None, description="The pin promotion (ad) ID.")
    pin_promotion_name: str | None = Field(default=None, description="The pin promotion (ad) name.")
    pin_promotion_status: str | None = Field(default=None, description="The pin promotion (ad) status.")
    pinterest_checkout_cost_per_action: float | None = Field(
        default=None, description="Cost per pinterest checkout conversion."
    )
    pinterest_checkout_roas: float | None = Field(
        default=None, description="Return on ad spend for pinterest checkout conversions."
    )
    product_group_ad_image_tag: str | None = Field(default=None, description="The product group's ad image tag.")
    product_group_ad_video_tag: str | None = Field(default=None, description="The product group's ad video tag.")
    product_group_id: str | None = Field(default=None, description="The product group ID.")
    product_group_status: str | None = Field(default=None, description="The product group status.")
    product_item_brand: str | None = Field(default=None, description="The product item brand.")
    product_item_currency: str | None = Field(default=None, description="The product item currency.")
    product_item_description: str | None = Field(default=None, description="The product item description.")
    product_item_image_url: str | None = Field(default=None, description="The product item image URL.")
    product_item_name: str | None = Field(default=None, description="The product item name.")
    product_item_pin_url: str | None = Field(default=None, description="The product item Pin URL.")
    product_item_price: float | None = Field(default=None, description="The product item price.")
    product_item_product_category: str | None = Field(default=None, description="The product item category.")
    product_item_product_type: str | None = Field(default=None, description="The product item type.")
    product_item_product_url: str | None = Field(default=None, description="The product item URL.")
    product_item_sale_price: float | None = Field(default=None, description="The product item sale price.")
    promo_id: str | None = Field(default=None, description="The promotion ID.")
    promo_name: str | None = Field(default=None, description="The promotion name.")
    quiz_completed: int | None = Field(default=None, description="Quiz completions.")
    quiz_completion_rate: float | None = Field(default=None, description="Quiz completion rate.")
    repin_1: int | None = Field(default=None, description="Paid saves (repins).")
    repin_2: int | None = Field(default=None, description="Earned saves (repins).")
    repin_rate: float | None = Field(default=None, description="Overall repin rate.")
    showcase_average_subpage_closeup_per_session: float | None = Field(
        default=None, description="Average showcase subpage closeups per session."
    )
    showcase_card_thumbnail_swipe_backward: int | None = Field(
        default=None, description="Showcase ad card thumbnail swipe backward."
    )
    showcase_card_thumbnail_swipe_forward: int | None = Field(
        default=None, description="Showcase ad card thumbnail swipe forward."
    )
    showcase_pin_clickthrough: int | None = Field(default=None, description="Showcase ad pin clickthrough.")
    showcase_subpage_clickthrough: int | None = Field(default=None, description="Showcase ad subpage clickthrough.")
    showcase_subpage_closeup: int | None = Field(default=None, description="Showcase ad subpage closeup.")
    showcase_subpage_impression: int | None = Field(default=None, description="Showcase ad subpage impression.")
    showcase_subpage_repin: int | None = Field(default=None, description="Showcase ad subpage repin.")
    showcase_subpage_swipe_left: int | None = Field(default=None, description="Showcase ad subpage swipe left.")
    showcase_subpage_swipe_right: int | None = Field(default=None, description="Showcase ad subpage swipe right.")
    showcase_subpin_clickthrough: int | None = Field(default=None, description="Showcase ad subpin clickthrough.")
    showcase_subpin_impression: int | None = Field(default=None, description="Showcase ad subpin impression.")
    showcase_subpin_repin: int | None = Field(default=None, description="Showcase ad subpin repin.")
    showcase_subpin_swipe_left: int | None = Field(default=None, description="Showcase ad subpin swipe left.")
    showcase_subpin_swipe_right: int | None = Field(default=None, description="Showcase ad subpin swipe right.")
    spend_in_dollar: float | None = Field(default=None, description="Total spend in dollars.")
    spend_in_micro_dollar: float | None = Field(default=None, description="Total spend in micro dollars.")
    standard_ad_feed_item_id: str | None = Field(
        default=None, description="The catalog feed item ID behind a standard ad."
    )
    total_add_to_cart_conversion_rate: float | None = Field(default=None, description="Add-to-cart conversion rate.")
    total_add_to_cart_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on desktop after an ad action on desktop."
    )
    total_add_to_cart_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on mobile after an ad action on desktop."
    )
    total_add_to_cart_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on tablet after an ad action on desktop."
    )
    total_add_to_cart_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on desktop after an ad action on mobile."
    )
    total_add_to_cart_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on mobile after an ad action on mobile."
    )
    total_add_to_cart_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on tablet after an ad action on mobile."
    )
    total_add_to_cart_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on desktop after an ad action on tablet."
    )
    total_add_to_cart_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on mobile after an ad action on tablet."
    )
    total_add_to_cart_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Add-to-cart conversions on tablet after an ad action on tablet."
    )
    total_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of add-to-cart conversions, in micro-dollars."
    )
    total_add_to_cart: int | None = Field(default=None, description="Add-to-cart conversions.")
    total_add_to_wishlist: int | None = Field(default=None, description="Total add-to-wishlist conversions.")
    total_app_install_conversion_rate: float | None = Field(default=None, description="App-install conversion rate.")
    total_app_install_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="App-install conversions on desktop after an ad action on desktop."
    )
    total_app_install_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="App-install conversions on mobile after an ad action on desktop."
    )
    total_app_install_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="App-install conversions on tablet after an ad action on desktop."
    )
    total_app_install_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="App-install conversions on desktop after an ad action on mobile."
    )
    total_app_install_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="App-install conversions on mobile after an ad action on mobile."
    )
    total_app_install_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="App-install conversions on tablet after an ad action on mobile."
    )
    total_app_install_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="App-install conversions on desktop after an ad action on tablet."
    )
    total_app_install_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="App-install conversions on mobile after an ad action on tablet."
    )
    total_app_install_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="App-install conversions on tablet after an ad action on tablet."
    )
    total_app_install_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of app-install conversions, in micro-dollars."
    )
    total_app_install: int | None = Field(default=None, description="App-install conversions.")
    total_checkout_conversion_rate: float | None = Field(default=None, description="Checkout conversion rate.")
    total_checkout_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Checkout conversions on desktop after an ad action on desktop."
    )
    total_checkout_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Checkout conversions on mobile after an ad action on desktop."
    )
    total_checkout_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Checkout conversions on tablet after an ad action on desktop."
    )
    total_checkout_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Checkout conversions on desktop after an ad action on mobile."
    )
    total_checkout_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Checkout conversions on mobile after an ad action on mobile."
    )
    total_checkout_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Checkout conversions on tablet after an ad action on mobile."
    )
    total_checkout_quantity: int | None = Field(default=None, description="Total order quantity from checkouts.")
    total_checkout_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Checkout conversions on desktop after an ad action on tablet."
    )
    total_checkout_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Checkout conversions on mobile after an ad action on tablet."
    )
    total_checkout_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Checkout conversions on tablet after an ad action on tablet."
    )
    total_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of checkout conversions, in micro-dollars."
    )
    total_checkout: int | None = Field(default=None, description="Checkout conversions.")
    total_click_add_to_cart_quantity: int | None = Field(
        default=None, description="Order quantity from add-to-cart conversions attributed to a Pin click."
    )
    total_click_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of add-to-cart conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_add_to_cart: int | None = Field(
        default=None, description="Add-to-cart conversions attributed to a Pin click."
    )
    total_click_app_install_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of app-install conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_app_install: int | None = Field(
        default=None, description="App-install conversions attributed to a Pin click."
    )
    total_click_checkout_quantity: int | None = Field(
        default=None, description="Order quantity from checkout conversions attributed to a Pin click."
    )
    total_click_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of checkout conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_checkout: int | None = Field(
        default=None, description="Checkout conversions attributed to a Pin click."
    )
    total_click_custom_quantity: int | None = Field(
        default=None, description="Order quantity from custom conversions attributed to a Pin click."
    )
    total_click_custom_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of custom conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_custom: int | None = Field(default=None, description="Custom conversions attributed to a Pin click.")
    total_click_lead_quantity: int | None = Field(
        default=None, description="Order quantity from lead conversions attributed to a Pin click."
    )
    total_click_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of lead conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_lead: int | None = Field(default=None, description="Lead conversions attributed to a Pin click.")
    total_click_page_visit_quantity: int | None = Field(
        default=None, description="Order quantity from page-visit conversions attributed to a Pin click."
    )
    total_click_page_visit_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of page-visit conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_page_visit: int | None = Field(
        default=None, description="Page-visit conversions attributed to a Pin click."
    )
    total_click_search_quantity: int | None = Field(
        default=None, description="Order quantity from search conversions attributed to a Pin click."
    )
    total_click_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of search conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_search: int | None = Field(default=None, description="Search conversions attributed to a Pin click.")
    total_click_signup_quantity: int | None = Field(
        default=None, description="Order quantity from signup conversions attributed to a Pin click."
    )
    total_click_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of signup conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_signup: int | None = Field(default=None, description="Signup conversions attributed to a Pin click.")
    total_click_unknown_quantity: int | None = Field(
        default=None, description="Order quantity from unknown conversions attributed to a Pin click."
    )
    total_click_unknown_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of unknown conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_unknown: int | None = Field(default=None, description="Unknown conversions attributed to a Pin click.")
    total_click_view_category_quantity: int | None = Field(
        default=None, description="Order quantity from view-category conversions attributed to a Pin click."
    )
    total_click_view_category_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of view-category conversions attributed to a Pin click, in micro-dollars.",
    )
    total_click_view_category: int | None = Field(
        default=None, description="View-category conversions attributed to a Pin click."
    )
    total_click_watch_video_quantity: int | None = Field(
        default=None, description="Order quantity from watch-video conversions attributed to a Pin click."
    )
    total_click_watch_video_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of watch-video conversions attributed to a Pin click, in micro-dollars."
    )
    total_click_watch_video: int | None = Field(
        default=None, description="Watch-video conversions attributed to a Pin click."
    )
    total_clickthrough: int | None = Field(default=None, description="Total Pin clicks (paid and earned).")
    total_conversions_quantity: int | None = Field(default=None, description="Total order quantity across conversions.")
    total_conversions_value_in_micro_dollar: float | None = Field(
        default=None, description="Total conversion value, in micro-dollars."
    )
    total_conversions: int | None = Field(default=None, description="Total conversions.")
    total_custom_conversion_rate: float | None = Field(default=None, description="Custom conversion rate.")
    total_custom_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Custom conversions on desktop after an ad action on desktop."
    )
    total_custom_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Custom conversions on mobile after an ad action on desktop."
    )
    total_custom_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Custom conversions on tablet after an ad action on desktop."
    )
    total_custom_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Custom conversions on desktop after an ad action on mobile."
    )
    total_custom_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Custom conversions on mobile after an ad action on mobile."
    )
    total_custom_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Custom conversions on tablet after an ad action on mobile."
    )
    total_custom_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Custom conversions on desktop after an ad action on tablet."
    )
    total_custom_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Custom conversions on mobile after an ad action on tablet."
    )
    total_custom_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Custom conversions on tablet after an ad action on tablet."
    )
    total_custom_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of custom conversions, in micro-dollars."
    )
    total_custom: int | None = Field(default=None, description="Custom conversions.")
    total_destination_views: int | None = Field(default=None, description="Total destination views.")
    total_engagement_add_to_cart_quantity: int | None = Field(
        default=None, description="Order quantity from add-to-cart conversions attributed to a Pin engagement."
    )
    total_engagement_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of add-to-cart conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_engagement_add_to_cart: int | None = Field(
        default=None, description="Add-to-cart conversions attributed to a Pin engagement."
    )
    total_engagement_app_install_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of app-install conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_engagement_app_install: int | None = Field(
        default=None, description="App-install conversions attributed to a Pin engagement."
    )
    total_engagement_checkout_quantity: int | None = Field(
        default=None, description="Order quantity from checkout conversions attributed to a Pin engagement."
    )
    total_engagement_checkout_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of checkout conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_engagement_checkout: int | None = Field(
        default=None, description="Checkout conversions attributed to a Pin engagement."
    )
    total_engagement_custom_quantity: int | None = Field(
        default=None, description="Order quantity from custom conversions attributed to a Pin engagement."
    )
    total_engagement_custom_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of custom conversions attributed to a Pin engagement, in micro-dollars."
    )
    total_engagement_custom: int | None = Field(
        default=None, description="Custom conversions attributed to a Pin engagement."
    )
    total_engagement_lead_quantity: int | None = Field(
        default=None, description="Order quantity from lead conversions attributed to a Pin engagement."
    )
    total_engagement_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of lead conversions attributed to a Pin engagement, in micro-dollars."
    )
    total_engagement_lead: int | None = Field(
        default=None, description="Lead conversions attributed to a Pin engagement."
    )
    total_engagement_page_visit_quantity: int | None = Field(
        default=None, description="Order quantity from page-visit conversions attributed to a Pin engagement."
    )
    total_engagement_page_visit_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of page-visit conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_engagement_page_visit: int | None = Field(
        default=None, description="Page-visit conversions attributed to a Pin engagement."
    )
    total_engagement_search_quantity: int | None = Field(
        default=None, description="Order quantity from search conversions attributed to a Pin engagement."
    )
    total_engagement_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of search conversions attributed to a Pin engagement, in micro-dollars."
    )
    total_engagement_search: int | None = Field(
        default=None, description="Search conversions attributed to a Pin engagement."
    )
    total_engagement_signup_quantity: int | None = Field(
        default=None, description="Order quantity from signup conversions attributed to a Pin engagement."
    )
    total_engagement_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of signup conversions attributed to a Pin engagement, in micro-dollars."
    )
    total_engagement_signup: int | None = Field(
        default=None, description="Signup conversions attributed to a Pin engagement."
    )
    total_engagement_unknown_quantity: int | None = Field(
        default=None, description="Order quantity from unknown conversions attributed to a Pin engagement."
    )
    total_engagement_unknown_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of unknown conversions attributed to a Pin engagement, in micro-dollars."
    )
    total_engagement_unknown: int | None = Field(
        default=None, description="Unknown conversions attributed to a Pin engagement."
    )
    total_engagement_view_category_quantity: int | None = Field(
        default=None, description="Order quantity from view-category conversions attributed to a Pin engagement."
    )
    total_engagement_view_category_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of view-category conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_engagement_view_category: int | None = Field(
        default=None, description="View-category conversions attributed to a Pin engagement."
    )
    total_engagement_watch_video_quantity: int | None = Field(
        default=None, description="Order quantity from watch-video conversions attributed to a Pin engagement."
    )
    total_engagement_watch_video_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of watch-video conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_engagement_watch_video: int | None = Field(
        default=None, description="Watch-video conversions attributed to a Pin engagement."
    )
    total_engagement: int | None = Field(default=None, description="Total engagements (paid and earned).")
    total_idea_pin_page_backward: int | None = Field(
        default=None, description="Total backward page swipes on Idea Pins."
    )
    total_idea_pin_page_forward: int | None = Field(default=None, description="Total forward page swipes on Idea Pins.")
    total_idea_pin_product_tag_visit: int | None = Field(
        default=None, description="Total product tag visits on Idea Pins."
    )
    total_impression_frequency: float | None = Field(
        default=None, description="Average number of impressions per reached user."
    )
    total_impression_user: int | None = Field(default=None, description="Total unique users reached.")
    total_impression: int | None = Field(default=None, description="Total impressions (paid and earned).")
    total_inapp_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of in-app add-to-cart conversions, in micro-dollars."
    )
    total_inapp_add_to_cart: int | None = Field(default=None, description="In-app add-to-cart conversions.")
    total_inapp_app_install_conversion_rate: float | None = Field(
        default=None, description="In-app app-install conversion rate."
    )
    total_inapp_app_install_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of in-app app-install conversions, in micro-dollars."
    )
    total_inapp_app_install: int | None = Field(default=None, description="In-app app-install conversions.")
    total_inapp_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of in-app checkout conversions, in micro-dollars."
    )
    total_inapp_checkout: int | None = Field(default=None, description="In-app checkout conversions.")
    total_inapp_click_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app add-to-cart conversions attributed to a Pin click, in micro-dollars.",
    )
    total_inapp_click_add_to_cart: int | None = Field(
        default=None, description="In-app add-to-cart conversions attributed to a Pin click."
    )
    total_inapp_click_app_install_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app app-install conversions attributed to a Pin click, in micro-dollars.",
    )
    total_inapp_click_app_install: int | None = Field(
        default=None, description="In-app app-install conversions attributed to a Pin click."
    )
    total_inapp_click_checkout_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app checkout conversions attributed to a Pin click, in micro-dollars.",
    )
    total_inapp_click_checkout: int | None = Field(
        default=None, description="In-app checkout conversions attributed to a Pin click."
    )
    total_inapp_click_search_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app search conversions attributed to a Pin click, in micro-dollars.",
    )
    total_inapp_click_search: int | None = Field(
        default=None, description="In-app search conversions attributed to a Pin click."
    )
    total_inapp_click_signup_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app signup conversions attributed to a Pin click, in micro-dollars.",
    )
    total_inapp_click_signup: int | None = Field(
        default=None, description="In-app signup conversions attributed to a Pin click."
    )
    total_inapp_click_unknown_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app unknown conversions attributed to a Pin click, in micro-dollars.",
    )
    total_inapp_click_unknown: int | None = Field(
        default=None, description="In-app unknown conversions attributed to a Pin click."
    )
    total_inapp_engagement_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app add-to-cart conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_inapp_engagement_add_to_cart: int | None = Field(
        default=None, description="In-app add-to-cart conversions attributed to a Pin engagement."
    )
    total_inapp_engagement_app_install_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app app-install conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_inapp_engagement_app_install: int | None = Field(
        default=None, description="In-app app-install conversions attributed to a Pin engagement."
    )
    total_inapp_engagement_checkout_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app checkout conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_inapp_engagement_checkout: int | None = Field(
        default=None, description="In-app checkout conversions attributed to a Pin engagement."
    )
    total_inapp_engagement_search_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app search conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_inapp_engagement_search: int | None = Field(
        default=None, description="In-app search conversions attributed to a Pin engagement."
    )
    total_inapp_engagement_signup_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app signup conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_inapp_engagement_signup: int | None = Field(
        default=None, description="In-app signup conversions attributed to a Pin engagement."
    )
    total_inapp_engagement_unknown_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app unknown conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_inapp_engagement_unknown: int | None = Field(
        default=None, description="In-app unknown conversions attributed to a Pin engagement."
    )
    total_inapp_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of in-app search conversions, in micro-dollars."
    )
    total_inapp_search: int | None = Field(default=None, description="In-app search conversions.")
    total_inapp_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of in-app signup conversions, in micro-dollars."
    )
    total_inapp_signup: int | None = Field(default=None, description="In-app signup conversions.")
    total_inapp_unknown_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of in-app unknown conversions, in micro-dollars."
    )
    total_inapp_unknown: int | None = Field(default=None, description="In-app unknown conversions.")
    total_inapp_view_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app add-to-cart conversions attributed to a Pin view, in micro-dollars.",
    )
    total_inapp_view_add_to_cart: int | None = Field(
        default=None, description="In-app add-to-cart conversions attributed to a Pin view."
    )
    total_inapp_view_app_install_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app app-install conversions attributed to a Pin view, in micro-dollars.",
    )
    total_inapp_view_app_install: int | None = Field(
        default=None, description="In-app app-install conversions attributed to a Pin view."
    )
    total_inapp_view_checkout_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app checkout conversions attributed to a Pin view, in micro-dollars.",
    )
    total_inapp_view_checkout: int | None = Field(
        default=None, description="In-app checkout conversions attributed to a Pin view."
    )
    total_inapp_view_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of in-app search conversions attributed to a Pin view, in micro-dollars."
    )
    total_inapp_view_search: int | None = Field(
        default=None, description="In-app search conversions attributed to a Pin view."
    )
    total_inapp_view_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of in-app signup conversions attributed to a Pin view, in micro-dollars."
    )
    total_inapp_view_signup: int | None = Field(
        default=None, description="In-app signup conversions attributed to a Pin view."
    )
    total_inapp_view_unknown_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of in-app unknown conversions attributed to a Pin view, in micro-dollars.",
    )
    total_inapp_view_unknown: int | None = Field(
        default=None, description="In-app unknown conversions attributed to a Pin view."
    )
    total_lead_conversion_rate: float | None = Field(default=None, description="Lead conversion rate.")
    total_lead_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Lead conversions on desktop after an ad action on desktop."
    )
    total_lead_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Lead conversions on mobile after an ad action on desktop."
    )
    total_lead_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Lead conversions on tablet after an ad action on desktop."
    )
    total_lead_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Lead conversions on desktop after an ad action on mobile."
    )
    total_lead_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Lead conversions on mobile after an ad action on mobile."
    )
    total_lead_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Lead conversions on tablet after an ad action on mobile."
    )
    total_lead_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Lead conversions on desktop after an ad action on tablet."
    )
    total_lead_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Lead conversions on mobile after an ad action on tablet."
    )
    total_lead_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Lead conversions on tablet after an ad action on tablet."
    )
    total_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of lead conversions, in micro-dollars."
    )
    total_lead: int | None = Field(default=None, description="Total lead conversions.")
    total_offline_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of offline checkout conversions, in micro-dollars."
    )
    total_offline_checkout: int | None = Field(default=None, description="Offline checkout conversions.")
    total_offline_click_checkout_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline checkout conversions attributed to a Pin click, in micro-dollars.",
    )
    total_offline_click_checkout: int | None = Field(
        default=None, description="Offline checkout conversions attributed to a Pin click."
    )
    total_offline_click_custom_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline custom conversions attributed to a Pin click, in micro-dollars.",
    )
    total_offline_click_custom: int | None = Field(
        default=None, description="Offline custom conversions attributed to a Pin click."
    )
    total_offline_click_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of offline lead conversions attributed to a Pin click, in micro-dollars."
    )
    total_offline_click_lead: int | None = Field(
        default=None, description="Offline lead conversions attributed to a Pin click."
    )
    total_offline_click_signup_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline signup conversions attributed to a Pin click, in micro-dollars.",
    )
    total_offline_click_signup: int | None = Field(
        default=None, description="Offline signup conversions attributed to a Pin click."
    )
    total_offline_click_unknown_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline unknown conversions attributed to a Pin click, in micro-dollars.",
    )
    total_offline_click_unknown: int | None = Field(
        default=None, description="Offline unknown conversions attributed to a Pin click."
    )
    total_offline_custom_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of offline custom conversions, in micro-dollars."
    )
    total_offline_custom: int | None = Field(default=None, description="Offline custom conversions.")
    total_offline_engagement_checkout_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline checkout conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_offline_engagement_checkout: int | None = Field(
        default=None, description="Offline checkout conversions attributed to a Pin engagement."
    )
    total_offline_engagement_custom_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline custom conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_offline_engagement_custom: int | None = Field(
        default=None, description="Offline custom conversions attributed to a Pin engagement."
    )
    total_offline_engagement_lead_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline lead conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_offline_engagement_lead: int | None = Field(
        default=None, description="Offline lead conversions attributed to a Pin engagement."
    )
    total_offline_engagement_signup_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline signup conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_offline_engagement_signup: int | None = Field(
        default=None, description="Offline signup conversions attributed to a Pin engagement."
    )
    total_offline_engagement_unknown_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline unknown conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_offline_engagement_unknown: int | None = Field(
        default=None, description="Offline unknown conversions attributed to a Pin engagement."
    )
    total_offline_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of offline lead conversions, in micro-dollars."
    )
    total_offline_lead: int | None = Field(default=None, description="Offline lead conversions.")
    total_offline_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of offline signup conversions, in micro-dollars."
    )
    total_offline_signup: int | None = Field(default=None, description="Offline signup conversions.")
    total_offline_unknown_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of offline unknown conversions, in micro-dollars."
    )
    total_offline_unknown: int | None = Field(default=None, description="Offline unknown conversions.")
    total_offline_view_checkout_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline checkout conversions attributed to a Pin view, in micro-dollars.",
    )
    total_offline_view_checkout: int | None = Field(
        default=None, description="Offline checkout conversions attributed to a Pin view."
    )
    total_offline_view_custom_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline custom conversions attributed to a Pin view, in micro-dollars.",
    )
    total_offline_view_custom: int | None = Field(
        default=None, description="Offline custom conversions attributed to a Pin view."
    )
    total_offline_view_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of offline lead conversions attributed to a Pin view, in micro-dollars."
    )
    total_offline_view_lead: int | None = Field(
        default=None, description="Offline lead conversions attributed to a Pin view."
    )
    total_offline_view_signup_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline signup conversions attributed to a Pin view, in micro-dollars.",
    )
    total_offline_view_signup: int | None = Field(
        default=None, description="Offline signup conversions attributed to a Pin view."
    )
    total_offline_view_unknown_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of offline unknown conversions attributed to a Pin view, in micro-dollars.",
    )
    total_offline_view_unknown: int | None = Field(
        default=None, description="Offline unknown conversions attributed to a Pin view."
    )
    total_page_visit_conversion_rate: float | None = Field(default=None, description="Page-visit conversion rate.")
    total_page_visit_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Page-visit conversions on desktop after an ad action on desktop."
    )
    total_page_visit_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Page-visit conversions on mobile after an ad action on desktop."
    )
    total_page_visit_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Page-visit conversions on tablet after an ad action on desktop."
    )
    total_page_visit_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Page-visit conversions on desktop after an ad action on mobile."
    )
    total_page_visit_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Page-visit conversions on mobile after an ad action on mobile."
    )
    total_page_visit_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Page-visit conversions on tablet after an ad action on mobile."
    )
    total_page_visit_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Page-visit conversions on desktop after an ad action on tablet."
    )
    total_page_visit_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Page-visit conversions on mobile after an ad action on tablet."
    )
    total_page_visit_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Page-visit conversions on tablet after an ad action on tablet."
    )
    total_page_visit: int | None = Field(default=None, description="Page-visit conversions.")
    total_pinterest_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of Pinterest checkout conversions, in micro-dollars."
    )
    total_pinterest_checkout: int | None = Field(default=None, description="Pinterest checkout conversions.")
    total_repin_rate: float | None = Field(default=None, description="Total save (repin) rate.")
    total_search_conversion_rate: float | None = Field(default=None, description="Search conversion rate.")
    total_search_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Search conversions on desktop after an ad action on desktop."
    )
    total_search_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Search conversions on mobile after an ad action on desktop."
    )
    total_search_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Search conversions on tablet after an ad action on desktop."
    )
    total_search_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Search conversions on desktop after an ad action on mobile."
    )
    total_search_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Search conversions on mobile after an ad action on mobile."
    )
    total_search_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Search conversions on tablet after an ad action on mobile."
    )
    total_search_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Search conversions on desktop after an ad action on tablet."
    )
    total_search_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Search conversions on mobile after an ad action on tablet."
    )
    total_search_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Search conversions on tablet after an ad action on tablet."
    )
    total_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of search conversions, in micro-dollars."
    )
    total_search: int | None = Field(default=None, description="Search conversions.")
    total_signup_conversion_rate: float | None = Field(default=None, description="Signup conversion rate.")
    total_signup_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Signup conversions on desktop after an ad action on desktop."
    )
    total_signup_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Signup conversions on mobile after an ad action on desktop."
    )
    total_signup_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Signup conversions on tablet after an ad action on desktop."
    )
    total_signup_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Signup conversions on desktop after an ad action on mobile."
    )
    total_signup_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Signup conversions on mobile after an ad action on mobile."
    )
    total_signup_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Signup conversions on tablet after an ad action on mobile."
    )
    total_signup_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Signup conversions on desktop after an ad action on tablet."
    )
    total_signup_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Signup conversions on mobile after an ad action on tablet."
    )
    total_signup_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Signup conversions on tablet after an ad action on tablet."
    )
    total_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of signup conversions, in micro-dollars."
    )
    total_signup: int | None = Field(default=None, description="Signup conversions.")
    total_subscribe: int | None = Field(default=None, description="Total subscribe conversions.")
    total_unknown_conversion_rate: float | None = Field(default=None, description="Unknown conversion rate.")
    total_unknown_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Unknown conversions on desktop after an ad action on desktop."
    )
    total_unknown_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Unknown conversions on mobile after an ad action on desktop."
    )
    total_unknown_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Unknown conversions on tablet after an ad action on desktop."
    )
    total_unknown_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Unknown conversions on desktop after an ad action on mobile."
    )
    total_unknown_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Unknown conversions on mobile after an ad action on mobile."
    )
    total_unknown_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Unknown conversions on tablet after an ad action on mobile."
    )
    total_unknown_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Unknown conversions on desktop after an ad action on tablet."
    )
    total_unknown_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Unknown conversions on mobile after an ad action on tablet."
    )
    total_unknown_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Unknown conversions on tablet after an ad action on tablet."
    )
    total_video_15sec_unique_views: int | None = Field(
        default=None, description="Total unique video views of at least 15 seconds (paid and earned)."
    )
    total_video_3sec_views: int | None = Field(
        default=None, description="Total video views of at least 3 seconds (paid and earned)."
    )
    total_video_avg_watchtime_in_second: float | None = Field(
        default=None, description="Average video watch time in seconds (paid and earned)."
    )
    total_video_mrc_views: int | None = Field(
        default=None, description="Total MRC-standard video views (paid and earned)."
    )
    total_video_p0_combined: int | None = Field(default=None, description="Total video starts (paid and earned).")
    total_video_p100_complete: int | None = Field(
        default=None, description="Total video views reaching 100% of length (paid and earned)."
    )
    total_video_p25_combined: int | None = Field(
        default=None, description="Video views reaching 25% of length (paid and earned)."
    )
    total_video_p50_combined: int | None = Field(
        default=None, description="Video views reaching 50% of length (paid and earned)."
    )
    total_video_p75_combined: int | None = Field(
        default=None, description="Video views reaching 75% of length (paid and earned)."
    )
    total_video_p95_combined: int | None = Field(
        default=None, description="Video views reaching 95% of length (paid and earned)."
    )
    total_view_add_to_cart_quantity: int | None = Field(
        default=None, description="Order quantity from add-to-cart conversions attributed to a Pin view."
    )
    total_view_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of add-to-cart conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_add_to_cart: int | None = Field(
        default=None, description="Add-to-cart conversions attributed to a Pin view."
    )
    total_view_app_install_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of app-install conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_app_install: int | None = Field(
        default=None, description="App-install conversions attributed to a Pin view."
    )
    total_view_category_conversion_rate: float | None = Field(
        default=None, description="View-category conversion rate."
    )
    total_view_category_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="View-category conversions on desktop after an ad action on desktop."
    )
    total_view_category_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="View-category conversions on mobile after an ad action on desktop."
    )
    total_view_category_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="View-category conversions on tablet after an ad action on desktop."
    )
    total_view_category_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="View-category conversions on desktop after an ad action on mobile."
    )
    total_view_category_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="View-category conversions on mobile after an ad action on mobile."
    )
    total_view_category_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="View-category conversions on tablet after an ad action on mobile."
    )
    total_view_category_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="View-category conversions on desktop after an ad action on tablet."
    )
    total_view_category_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="View-category conversions on mobile after an ad action on tablet."
    )
    total_view_category_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="View-category conversions on tablet after an ad action on tablet."
    )
    total_view_category_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of view-category conversions, in micro-dollars."
    )
    total_view_category: int | None = Field(default=None, description="View-category conversions.")
    total_view_checkout_quantity: int | None = Field(
        default=None, description="Order quantity from checkout conversions attributed to a Pin view."
    )
    total_view_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of checkout conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_checkout: int | None = Field(default=None, description="Checkout conversions attributed to a Pin view.")
    total_view_custom_quantity: int | None = Field(
        default=None, description="Order quantity from custom conversions attributed to a Pin view."
    )
    total_view_custom_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of custom conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_custom: int | None = Field(default=None, description="Custom conversions attributed to a Pin view.")
    total_view_lead_quantity: int | None = Field(
        default=None, description="Order quantity from lead conversions attributed to a Pin view."
    )
    total_view_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of lead conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_lead: int | None = Field(default=None, description="Lead conversions attributed to a Pin view.")
    total_view_page_visit_quantity: int | None = Field(
        default=None, description="Order quantity from page-visit conversions attributed to a Pin view."
    )
    total_view_page_visit_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of page-visit conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_page_visit: int | None = Field(
        default=None, description="Page-visit conversions attributed to a Pin view."
    )
    total_view_search_quantity: int | None = Field(
        default=None, description="Order quantity from search conversions attributed to a Pin view."
    )
    total_view_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of search conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_search: int | None = Field(default=None, description="Search conversions attributed to a Pin view.")
    total_view_signup_quantity: int | None = Field(
        default=None, description="Order quantity from signup conversions attributed to a Pin view."
    )
    total_view_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of signup conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_signup: int | None = Field(default=None, description="Signup conversions attributed to a Pin view.")
    total_view_unknown_quantity: int | None = Field(
        default=None, description="Order quantity from unknown conversions attributed to a Pin view."
    )
    total_view_unknown_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of unknown conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_unknown: int | None = Field(default=None, description="Unknown conversions attributed to a Pin view.")
    total_view_view_category_quantity: int | None = Field(
        default=None, description="Order quantity from view-category conversions attributed to a Pin view."
    )
    total_view_view_category_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of view-category conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_view_category: int | None = Field(
        default=None, description="View-category conversions attributed to a Pin view."
    )
    total_view_watch_video_quantity: int | None = Field(
        default=None, description="Order quantity from watch-video conversions attributed to a Pin view."
    )
    total_view_watch_video_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of watch-video conversions attributed to a Pin view, in micro-dollars."
    )
    total_view_watch_video: int | None = Field(
        default=None, description="Watch-video conversions attributed to a Pin view."
    )
    total_watch_video_conversion_rate: float | None = Field(default=None, description="Watch-video conversion rate.")
    total_watch_video_desktop_action_to_desktop_conversion: int | None = Field(
        default=None, description="Watch-video conversions on desktop after an ad action on desktop."
    )
    total_watch_video_desktop_action_to_mobile_conversion: int | None = Field(
        default=None, description="Watch-video conversions on mobile after an ad action on desktop."
    )
    total_watch_video_desktop_action_to_tablet_conversion: int | None = Field(
        default=None, description="Watch-video conversions on tablet after an ad action on desktop."
    )
    total_watch_video_mobile_action_to_desktop_conversion: int | None = Field(
        default=None, description="Watch-video conversions on desktop after an ad action on mobile."
    )
    total_watch_video_mobile_action_to_mobile_conversion: int | None = Field(
        default=None, description="Watch-video conversions on mobile after an ad action on mobile."
    )
    total_watch_video_mobile_action_to_tablet_conversion: int | None = Field(
        default=None, description="Watch-video conversions on tablet after an ad action on mobile."
    )
    total_watch_video_tablet_action_to_desktop_conversion: int | None = Field(
        default=None, description="Watch-video conversions on desktop after an ad action on tablet."
    )
    total_watch_video_tablet_action_to_mobile_conversion: int | None = Field(
        default=None, description="Watch-video conversions on mobile after an ad action on tablet."
    )
    total_watch_video_tablet_action_to_tablet_conversion: int | None = Field(
        default=None, description="Watch-video conversions on tablet after an ad action on tablet."
    )
    total_watch_video_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of watch-video conversions, in micro-dollars."
    )
    total_watch_video: int | None = Field(default=None, description="Watch-video conversions.")
    total_web_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web add-to-cart conversions, in micro-dollars."
    )
    total_web_add_to_cart: int | None = Field(default=None, description="Web add-to-cart conversions.")
    total_web_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web checkout conversions, in micro-dollars."
    )
    total_web_checkout: int | None = Field(default=None, description="Web checkout conversions.")
    total_web_click_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web add-to-cart conversions attributed to a Pin click, in micro-dollars.",
    )
    total_web_click_add_to_cart: int | None = Field(
        default=None, description="Web add-to-cart conversions attributed to a Pin click."
    )
    total_web_click_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web checkout conversions attributed to a Pin click, in micro-dollars."
    )
    total_web_click_checkout: int | None = Field(
        default=None, description="Web checkout conversions attributed to a Pin click."
    )
    total_web_click_custom_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web custom conversions attributed to a Pin click, in micro-dollars."
    )
    total_web_click_custom: int | None = Field(
        default=None, description="Web custom conversions attributed to a Pin click."
    )
    total_web_click_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web lead conversions attributed to a Pin click, in micro-dollars."
    )
    total_web_click_lead: int | None = Field(
        default=None, description="Web lead conversions attributed to a Pin click."
    )
    total_web_click_page_visit_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web page-visit conversions attributed to a Pin click, in micro-dollars.",
    )
    total_web_click_page_visit: int | None = Field(
        default=None, description="Web page-visit conversions attributed to a Pin click."
    )
    total_web_click_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web search conversions attributed to a Pin click, in micro-dollars."
    )
    total_web_click_search: int | None = Field(
        default=None, description="Web search conversions attributed to a Pin click."
    )
    total_web_click_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web signup conversions attributed to a Pin click, in micro-dollars."
    )
    total_web_click_signup: int | None = Field(
        default=None, description="Web signup conversions attributed to a Pin click."
    )
    total_web_click_unknown_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web unknown conversions attributed to a Pin click, in micro-dollars."
    )
    total_web_click_unknown: int | None = Field(
        default=None, description="Web unknown conversions attributed to a Pin click."
    )
    total_web_click_view_category_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web view-category conversions attributed to a Pin click, in micro-dollars.",
    )
    total_web_click_view_category: int | None = Field(
        default=None, description="Web view-category conversions attributed to a Pin click."
    )
    total_web_click_watch_video_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web watch-video conversions attributed to a Pin click, in micro-dollars.",
    )
    total_web_click_watch_video: int | None = Field(
        default=None, description="Web watch-video conversions attributed to a Pin click."
    )
    total_web_custom_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web custom conversions, in micro-dollars."
    )
    total_web_custom: int | None = Field(default=None, description="Web custom conversions.")
    total_web_engagement_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web add-to-cart conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_add_to_cart: int | None = Field(
        default=None, description="Web add-to-cart conversions attributed to a Pin engagement."
    )
    total_web_engagement_checkout_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web checkout conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_checkout: int | None = Field(
        default=None, description="Web checkout conversions attributed to a Pin engagement."
    )
    total_web_engagement_custom_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web custom conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_custom: int | None = Field(
        default=None, description="Web custom conversions attributed to a Pin engagement."
    )
    total_web_engagement_lead_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web lead conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_lead: int | None = Field(
        default=None, description="Web lead conversions attributed to a Pin engagement."
    )
    total_web_engagement_page_visit_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web page-visit conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_page_visit: int | None = Field(
        default=None, description="Web page-visit conversions attributed to a Pin engagement."
    )
    total_web_engagement_search_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web search conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_search: int | None = Field(
        default=None, description="Web search conversions attributed to a Pin engagement."
    )
    total_web_engagement_signup_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web signup conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_signup: int | None = Field(
        default=None, description="Web signup conversions attributed to a Pin engagement."
    )
    total_web_engagement_unknown_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web unknown conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_unknown: int | None = Field(
        default=None, description="Web unknown conversions attributed to a Pin engagement."
    )
    total_web_engagement_view_category_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web view-category conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_view_category: int | None = Field(
        default=None, description="Web view-category conversions attributed to a Pin engagement."
    )
    total_web_engagement_watch_video_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web watch-video conversions attributed to a Pin engagement, in micro-dollars.",
    )
    total_web_engagement_watch_video: int | None = Field(
        default=None, description="Web watch-video conversions attributed to a Pin engagement."
    )
    total_web_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web lead conversions, in micro-dollars."
    )
    total_web_lead: int | None = Field(default=None, description="Web lead conversions.")
    total_web_page_visit_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web page-visit conversions, in micro-dollars."
    )
    total_web_page_visit: int | None = Field(default=None, description="Web page-visit conversions.")
    total_web_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web search conversions, in micro-dollars."
    )
    total_web_search: int | None = Field(default=None, description="Web search conversions.")
    total_web_sessions: int | None = Field(default=None, description="Total web sessions (paid and earned).")
    total_web_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web signup conversions, in micro-dollars."
    )
    total_web_signup: int | None = Field(default=None, description="Web signup conversions.")
    total_web_unknown_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web unknown conversions, in micro-dollars."
    )
    total_web_unknown: int | None = Field(default=None, description="Web unknown conversions.")
    total_web_view_add_to_cart_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web add-to-cart conversions attributed to a Pin view, in micro-dollars.",
    )
    total_web_view_add_to_cart: int | None = Field(
        default=None, description="Web add-to-cart conversions attributed to a Pin view."
    )
    total_web_view_category_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web view-category conversions, in micro-dollars."
    )
    total_web_view_category: int | None = Field(default=None, description="Web view-category conversions.")
    total_web_view_checkout_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web checkout conversions attributed to a Pin view, in micro-dollars."
    )
    total_web_view_checkout: int | None = Field(
        default=None, description="Web checkout conversions attributed to a Pin view."
    )
    total_web_view_custom_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web custom conversions attributed to a Pin view, in micro-dollars."
    )
    total_web_view_custom: int | None = Field(
        default=None, description="Web custom conversions attributed to a Pin view."
    )
    total_web_view_lead_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web lead conversions attributed to a Pin view, in micro-dollars."
    )
    total_web_view_lead: int | None = Field(default=None, description="Web lead conversions attributed to a Pin view.")
    total_web_view_page_visit_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web page-visit conversions attributed to a Pin view, in micro-dollars.",
    )
    total_web_view_page_visit: int | None = Field(
        default=None, description="Web page-visit conversions attributed to a Pin view."
    )
    total_web_view_search_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web search conversions attributed to a Pin view, in micro-dollars."
    )
    total_web_view_search: int | None = Field(
        default=None, description="Web search conversions attributed to a Pin view."
    )
    total_web_view_signup_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web signup conversions attributed to a Pin view, in micro-dollars."
    )
    total_web_view_signup: int | None = Field(
        default=None, description="Web signup conversions attributed to a Pin view."
    )
    total_web_view_unknown_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web unknown conversions attributed to a Pin view, in micro-dollars."
    )
    total_web_view_unknown: int | None = Field(
        default=None, description="Web unknown conversions attributed to a Pin view."
    )
    total_web_view_view_category_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web view-category conversions attributed to a Pin view, in micro-dollars.",
    )
    total_web_view_view_category: int | None = Field(
        default=None, description="Web view-category conversions attributed to a Pin view."
    )
    total_web_view_watch_video_value_in_micro_dollar: float | None = Field(
        default=None,
        description="Order value of web watch-video conversions attributed to a Pin view, in micro-dollars.",
    )
    total_web_view_watch_video: int | None = Field(
        default=None, description="Web watch-video conversions attributed to a Pin view."
    )
    total_web_watch_video_value_in_micro_dollar: float | None = Field(
        default=None, description="Order value of web watch-video conversions, in micro-dollars."
    )
    total_web_watch_video: int | None = Field(default=None, description="Web watch-video conversions.")
    video_15sec_unique_views_1: int | None = Field(
        default=None, description="Paid unique video views of at least 15 seconds."
    )
    video_15sec_unique_views_2: int | None = Field(
        default=None, description="Earned unique video views of at least 15 seconds."
    )
    video_3sec_views_1: int | None = Field(default=None, description="Paid video views of at least 3 seconds.")
    video_3sec_views_2: int | None = Field(default=None, description="Earned video views of at least 3 seconds.")
    video_avg_watchtime_in_second_1: float | None = Field(
        default=None, description="Paid average video watch time in seconds."
    )
    video_avg_watchtime_in_second_2: float | None = Field(
        default=None, description="Earned average video watch time in seconds."
    )
    video_length: float | None = Field(default=None, description="The video length.")
    video_mrc_views_1: int | None = Field(default=None, description="Paid MRC-standard video views.")
    video_mrc_views_2: int | None = Field(default=None, description="Earned MRC-standard video views.")
    video_p0_combined_1: int | None = Field(default=None, description="Paid video starts.")
    video_p0_combined_2: int | None = Field(default=None, description="Earned video starts.")
    video_p100_complete_1: int | None = Field(default=None, description="Paid video views reaching 100% of length.")
    video_p100_complete_2: int | None = Field(default=None, description="Earned video views reaching 100% of length.")
    video_p25_combined_1: int | None = Field(default=None, description="Video views reaching 25% of length (paid).")
    video_p25_combined_2: int | None = Field(default=None, description="Video views reaching 25% of length (earned).")
    video_p50_combined_1: int | None = Field(default=None, description="Video views reaching 50% of length (paid).")
    video_p50_combined_2: int | None = Field(default=None, description="Video views reaching 50% of length (earned).")
    video_p75_combined_1: int | None = Field(default=None, description="Video views reaching 75% of length (paid).")
    video_p75_combined_2: int | None = Field(default=None, description="Video views reaching 75% of length (earned).")
    video_p95_combined_1: int | None = Field(default=None, description="Video views reaching 95% of length (paid).")
    video_p95_combined_2: int | None = Field(default=None, description="Video views reaching 95% of length (earned).")
    video_spend_in_dollar: float | None = Field(default=None, description="Spend on video ads, in dollars.")
    web_add_to_cart_cost_per_action: float | None = Field(
        default=None, description="Cost per web add-to-cart conversion."
    )
    web_add_to_cart_roas: float | None = Field(
        default=None, description="Return on ad spend for web add-to-cart conversions."
    )
    web_checkout_cost_per_action: float | None = Field(default=None, description="Cost per web checkout conversion.")
    web_checkout_roas: float | None = Field(
        default=None, description="Return on ad spend for web checkout conversions."
    )
    web_custom_cost_per_action: float | None = Field(default=None, description="Cost per web custom conversion.")
    web_custom_roas: float | None = Field(default=None, description="Return on ad spend for web custom conversions.")
    web_lead_cost_per_action: float | None = Field(default=None, description="Cost per web lead conversion.")
    web_lead_roas: float | None = Field(default=None, description="Return on ad spend for web lead conversions.")
    web_page_visit_cost_per_action: float | None = Field(
        default=None, description="Cost per web page-visit conversion."
    )
    web_page_visit_roas: float | None = Field(
        default=None, description="Return on ad spend for web page-visit conversions."
    )
    web_search_cost_per_action: float | None = Field(default=None, description="Cost per web search conversion.")
    web_search_roas: float | None = Field(default=None, description="Return on ad spend for web search conversions.")
    web_sessions_1: int | None = Field(default=None, description="Paid web sessions.")
    web_sessions_2: int | None = Field(default=None, description="Earned web sessions.")
    web_signup_cost_per_action: float | None = Field(default=None, description="Cost per web signup conversion.")
    web_signup_roas: float | None = Field(default=None, description="Return on ad spend for web signup conversions.")
    web_unknown_cost_per_action: float | None = Field(default=None, description="Cost per web unknown conversion.")
    web_unknown_roas: float | None = Field(default=None, description="Return on ad spend for web unknown conversions.")
    web_view_category_cost_per_action: float | None = Field(
        default=None, description="Cost per web view-category conversion."
    )
    web_view_category_roas: float | None = Field(
        default=None, description="Return on ad spend for web view-category conversions."
    )
    web_watch_video_cost_per_action: float | None = Field(
        default=None, description="Cost per web watch-video conversion."
    )
    web_watch_video_roas: float | None = Field(
        default=None, description="Return on ad spend for web watch-video conversions."
    )

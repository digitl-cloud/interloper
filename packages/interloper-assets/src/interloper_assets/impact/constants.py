BASE_URL = "https://api.impact.com"

# Impact "ReportExport" report IDs used by the performance/SKU report assets.
# (The full list of valid IDs for an account is returned by the /Reports endpoint.)
REPORT_EXPORT_IDS = {
    "actions_by_sku": "adv_action_list_sku_pm_only",
    "performance": "att_adv_performance_by_day_pm_only",
    "performance_by_ad": "adv_performance_by_ad_pm_only",
    "performance_by_domain": "att_adv_performance_by_ref_domain_pm_only",
    "performance_by_shared_id": "att_adv_performance_by_shared_id_pm_only",
    "performance_by_io": "att_adv_performance_by_IO",
}

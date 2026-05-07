<script setup lang="ts">
definePageMeta({ layout: 'analytics' })

const dashboards = [
    {
        title: 'Campaign Performance',
        description: 'Track spend, conversions, CTR, and ROAS across all active marketing campaigns. Includes channel mix, funnel analysis, and engagement heatmaps.',
        icon: 'i-lucide-target',
        to: '/analytics/campaigns',
        stats: [
            { label: 'Active Campaigns', value: '12' },
            { label: 'Total Spend', value: '$118.2K' },
            { label: 'ROAS', value: '4.7x' },
        ],
        tags: ['Real-time', 'Multi-channel'],
    },
    {
        title: 'Marketing Mix Modeling',
        description: 'Bayesian media mix model decomposing revenue into channel contributions. Explore saturation curves, adstock decay, and budget optimization scenarios.',
        icon: 'i-lucide-blend',
        to: '/analytics/mmm',
        stats: [
            { label: 'Model R²', value: '0.94' },
            { label: 'Incremental Rev', value: '$8.2M' },
            { label: 'Opt. Lift', value: '+14%' },
        ],
        tags: ['Bayesian', '52-week'],
    },
]

const inactiveDashboards = [
    {
        title: 'Attribution Modeling',
        description: 'Multi-touch attribution across the customer journey. Compare first-touch, last-touch, linear, and data-driven models to understand true channel impact.',
        icon: 'i-lucide-git-branch',
        tags: ['Multi-touch', 'Customer Journey'],
    },
    {
        title: 'Audience Segmentation',
        description: 'Cluster analysis of customer cohorts by behavior, demographics, and lifetime value. Identify high-value segments and optimize targeting strategies.',
        icon: 'i-lucide-users',
        tags: ['Clustering', 'LTV'],
    },
    {
        title: 'Customer Purchase Prediction',
        description: 'Propensity models scoring customers by likelihood to convert, churn, or upgrade. Prioritize outreach with real-time prediction feeds and segment-level insights.',
        icon: 'i-lucide-shopping-cart',
        tags: ['Propensity', 'ML Scoring'],
    },
    {
        title: 'Predictive Forecasting',
        description: 'Time-series forecasting for revenue, spend efficiency, and conversion volume. Scenario planning with confidence intervals and seasonal adjustments.',
        icon: 'i-lucide-telescope',
        tags: ['Time-series', 'Scenarios'],
    },
]
</script>

<template>
    <div class="h-full overflow-y-auto">
        <div class="max-w-[1200px] mx-auto px-4 py-8 space-y-8">
            <!-- Dashboard cards -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <NuxtLink
                    v-for="db in dashboards"
                    :key="db.title"
                    :to="db.to"
                    class="group rounded-xl border border-default bg-elevated/40 backdrop-blur p-6 space-y-4 transition-all hover:border-primary hover:shadow-lg hover:shadow-primary/5"
                >
                    <!-- Title row -->
                    <div class="flex items-start justify-between">
                        <div class="flex items-center gap-3">
                            <div class="flex items-center justify-center size-10 rounded-lg bg-primary/10 text-primary">
                                <UIcon :name="db.icon" class="size-5" />
                            </div>
                            <h2 class="text-lg font-semibold group-hover:text-primary transition-colors">{{ db.title }}</h2>
                        </div>
                        <UIcon name="i-lucide-arrow-right" class="size-4 text-muted opacity-0 group-hover:opacity-100 transition-opacity mt-1" />
                    </div>

                    <!-- Description -->
                    <p class="text-sm text-muted leading-relaxed">{{ db.description }}</p>

                    <!-- Stats -->
                    <div class="grid grid-cols-3 gap-3">
                        <div
                            v-for="stat in db.stats"
                            :key="stat.label"
                            class="rounded-lg bg-elevated/50 px-3 py-2"
                        >
                            <p class="text-xs text-muted uppercase tracking-wide">{{ stat.label }}</p>
                            <p class="text-sm font-semibold mt-0.5">{{ stat.value }}</p>
                        </div>
                    </div>

                    <!-- Tags -->
                    <div class="flex gap-2">
                        <UBadge v-for="tag in db.tags" :key="tag" variant="subtle" size="sm">{{ tag }}</UBadge>
                    </div>
                </NuxtLink>

                <!-- Inactive cards -->
                <div
                    v-for="db in inactiveDashboards"
                    :key="db.title"
                    class="relative rounded-xl border border-dashed border-default bg-elevated/20 p-6 space-y-4 opacity-60"
                >
                    <div class="absolute top-4 right-4">
                        <UBadge color="neutral" variant="subtle" size="xs">Not Available</UBadge>
                    </div>

                    <div class="flex items-center gap-3">
                        <div class="flex items-center justify-center size-10 rounded-lg bg-muted/10 text-muted">
                            <UIcon :name="db.icon" class="size-5" />
                        </div>
                        <h2 class="text-lg font-semibold text-muted">{{ db.title }}</h2>
                    </div>

                    <p class="text-sm text-muted/70 leading-relaxed">{{ db.description }}</p>

                    <div class="flex gap-2">
                        <UBadge v-for="tag in db.tags" :key="tag" color="neutral" variant="subtle" size="sm">{{ tag }}</UBadge>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

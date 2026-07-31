<script setup lang="ts">
/**
 * UBreadcrumb with the app's crumb conventions wired in: build items with
 * titleCrumb (eyebrow-styled page title) and entityCrumb (rendered as an
 * EntityBadge) from utils/breadcrumb.
 */
import type { BreadcrumbItem } from '@nuxt/ui'

defineProps<{
    items: BreadcrumbItem[]
}>()
</script>

<template>
    <!-- min-height matches the EntityBadge rows so title-only breadcrumbs sit at the same Y on every page. -->
    <UBreadcrumb :items="items"
                 :ui="{ list: 'min-h-6' }">
        <!-- Custom item slots aren't in UBreadcrumb's slot types, so the scope needs a cast. -->
        <template #entity="scope">
            <EntityBadge :icon="(scope as { item: BreadcrumbItem }).item.icon"
                         :label="(scope as { item: BreadcrumbItem }).item.label ?? ''" />
        </template>
    </UBreadcrumb>
</template>

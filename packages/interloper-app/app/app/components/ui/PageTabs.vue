<script setup lang="ts">
/**
 * Top-level tab strip of a full-bleed page: flush under the navbar with its
 * rule across the whole panel, the active tab's content framed below — the
 * dashboard template's shape for a page split into views. Each view goes in
 * a slot named after its tab's value.
 */
import type { TabsItem } from '@nuxt/ui'

const tab = defineModel<string>({ required: true })

defineProps<{
    items: TabsItem[]
}>()
</script>

<template>
    <UTabs :items="items"
           :model-value="tab"
           :content="false"
           variant="link"
           class="shrink-0"
           :ui="{ list: 'px-6' }"
           @update:model-value="tab = String($event)" />
    <PageFrame>
        <slot :name="tab" />
    </PageFrame>
</template>

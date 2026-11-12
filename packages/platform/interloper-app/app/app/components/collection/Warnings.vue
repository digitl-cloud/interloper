<script setup lang="ts">
/**
 * An asset's configuration warnings as a list, one row each: the content of
 * the warning tooltips on the graph's cards and in the collection table.
 */
import type { AssetWarning } from '~/composables/warnings'

defineProps<{
    warnings: AssetWarning[]
}>()

const appConfig = useAppConfig()
</script>

<template>
    <ul class="flex flex-col gap-1.5 py-1">
        <li v-for="(warning, i) in warnings"
            :key="i"
            class="flex items-center gap-2">
            <UIcon :name="appConfig.ui.icons.warning"
                   class="size-3.5 shrink-0 text-warning" />
            <span>{{ warning.message }}</span>
        </li>
    </ul>
</template>

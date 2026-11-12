<script setup lang="ts">
/** The components that reference another, one row each: icon, name and kind, in an inset list. */
import type { UsedByRef } from '~/utils/apiErrors'

defineProps<{ referrers: UsedByRef[] }>()
</script>

<template>
    <div class="divide-y divide-default overflow-hidden rounded-lg ring ring-default">
        <div v-for="r in referrers"
             :key="r.id"
             class="flex items-center gap-2 px-3 py-2 text-sm">
            <UIcon :name="componentIcon(r.key)"
                   class="size-4 shrink-0 text-muted" />
            <span class="truncate font-medium">{{ r.name ?? r.key }}</span>
            <span class="ml-auto shrink-0 text-xs text-muted capitalize">{{ r.kind }}</span>
        </div>
    </div>
</template>

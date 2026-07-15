<script setup lang="ts">
import type { UsedByRef } from '~/utils/apiErrors'

defineProps<{ referrers: UsedByRef[] }>()
</script>

<template>
    <div class="border border-default rounded-lg divide-y divide-default overflow-hidden">
        <div v-for="r in referrers"
             :key="r.id"
             class="flex items-center gap-2 px-3 py-2 text-sm">
            <UIcon :name="componentIcon(r.key)"
                   class="size-4 shrink-0 text-muted" />
            <span class="font-medium truncate">{{ r.name ?? r.key }}</span>
            <span class="ml-auto text-xs text-muted capitalize shrink-0">{{ r.kind }}</span>
        </div>
    </div>
</template>

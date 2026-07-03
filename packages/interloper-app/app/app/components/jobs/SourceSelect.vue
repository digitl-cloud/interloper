<script setup lang="ts">
/**
 * Checkbox multi-select for choosing which sources to include in a job.
 */
import type { Source } from '~/types/source'

const selectedIds = defineModel<string[]>({ default: () => [] })

defineProps<{
    sources: Source[]
}>()

function sourceIcon(source: Source): string {
    return componentIcon(source.key)
}

function toggle(id: string) {
    const idx = selectedIds.value.indexOf(id)
    if (idx >= 0) selectedIds.value.splice(idx, 1)
    else selectedIds.value.push(id)
}

function selectAll(sources: Source[]) {
    selectedIds.value = sources.map(s => s.id)
}

function deselectAll() {
    selectedIds.value = []
}
</script>

<template>
    <div class="flex flex-col gap-3">
        <div class="flex items-center justify-between">
            <span class="text-sm text-muted">
                {{ selectedIds.length }} of {{ sources.length }} sources selected
            </span>
            <div class="flex gap-2">
                <UButton size="xs"
                         variant="ghost"
                         label="Select all"
                         @click="selectAll(sources)" />
                <UButton size="xs"
                         variant="ghost"
                         label="Deselect all"
                         @click="deselectAll()" />
            </div>
        </div>

        <InlineEmptyState v-if="sources.length === 0"
                          icon="i-lucide-plug"
                          message="No sources configured yet."
                          action-label="Go to Sources"
                          @action="navigateTo('/sources')" />

        <div v-else
             class="flex flex-col gap-2.5">
            <SelectionCard v-for="source in sources"
                           :key="source.id"
                           :selected="selectedIds.includes(source.id)"
                           class="flex items-center gap-3 px-4 py-3"
                           @select="toggle(source.id)">
                <UCheckbox :model-value="selectedIds.includes(source.id)"
                           @click.stop
                           @update:model-value="toggle(source.id)" />
                <div class="size-10 shrink-0 rounded-[11px] border border-default bg-default flex items-center justify-center">
                    <UIcon :name="sourceIcon(source)"
                           class="size-6" />
                </div>
                <div class="flex flex-col min-w-0">
                    <span class="text-[14.5px] font-semibold text-highlighted truncate">{{ source.name }}</span>
                    <span class="text-xs text-dimmed truncate">{{ source.key }}</span>
                </div>
                <UBadge color="neutral"
                        variant="subtle"
                        size="xs"
                        class="ml-auto">
                    {{ source.assets.length }} asset{{ source.assets.length !== 1 ? 's' : '' }}
                </UBadge>
            </SelectionCard>
        </div>
    </div>
</template>

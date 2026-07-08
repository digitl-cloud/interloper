<script setup lang="ts">
/**
 * Checkbox multi-select over components of mixed kinds (sources, assets,
 * jobs) — the watch/target pickers of the hook wizard.
 */
import type { ComponentRecord } from '~/types/component'

const selectedIds = defineModel<string[]>({ default: () => [] })

defineProps<{
    components: ComponentRecord[]
    /** Noun for the counter line, e.g. "watched" / "targeted". */
    noun?: string
}>()

function toggle(id: string) {
    const idx = selectedIds.value.indexOf(id)
    if (idx >= 0) selectedIds.value.splice(idx, 1)
    else selectedIds.value.push(id)
}
</script>

<template>
    <div class="flex flex-col gap-3">
        <span class="text-sm text-muted">
            {{ selectedIds.length }} of {{ components.length }} components {{ noun ?? 'selected' }}
        </span>

        <InlineEmptyState v-if="components.length === 0"
                          icon="i-lucide-plug"
                          message="No sources, assets or jobs configured yet."
                          action-label="Go to Sources"
                          @action="navigateTo('/sources')" />

        <div v-else
             class="flex flex-col gap-2.5">
            <SelectionCard v-for="component in components"
                           :key="component.id"
                           :selected="selectedIds.includes(component.id)"
                           class="flex items-center gap-3 px-4 py-3"
                           @select="toggle(component.id)">
                <UCheckbox :model-value="selectedIds.includes(component.id)"
                           @click.stop
                           @update:model-value="toggle(component.id)" />
                <div class="size-10 shrink-0 rounded-[11px] border border-default bg-default flex items-center justify-center">
                    <UIcon :name="componentIcon(component.key)"
                           class="size-6" />
                </div>
                <div class="flex flex-col min-w-0">
                    <span class="text-[14.5px] font-semibold text-highlighted truncate">{{ component.name }}</span>
                    <span class="text-xs text-dimmed truncate">{{ component.key }}</span>
                </div>
                <UBadge color="neutral"
                        variant="subtle"
                        size="xs"
                        class="ml-auto capitalize">
                    {{ component.kind }}
                </UBadge>
            </SelectionCard>
        </div>
    </div>
</template>

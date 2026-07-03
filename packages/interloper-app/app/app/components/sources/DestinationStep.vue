<script setup lang="ts">
/**
 * Step for selecting destinations for a source.
 *
 * Behaves as a multiselect: lists all existing destinations, user toggles
 * which ones to attach. A "Create new" button opens a nested drawer with
 * the DestinationsStepper to collect config for a new destination (created
 * when the source is saved).
 *
 * This step is optional — a source can exist without destinations.
 */

/** IDs of destinations to attach. */
const selectedIds = defineModel<string[]>('selectedIds', { default: () => [] })

const props = defineProps<{
    /**
     * Compatible destination keys from the source definition.
     * If empty, all destination types are available.
     */
    compatibleKeys?: string[]
}>()

const catalogStore = useCatalogStore()
const destinationsStore = useDestinationsStore()

const drawerOpen = ref(false)
const destStepperRef = ref<any>(null)
const loading = ref(false)

// ── Load destinations ───────────────────────────────────────────

onMounted(async () => {
    if (destinationsStore.destinations.length === 0 && !destinationsStore.loading) {
        loading.value = true
        try {
            await destinationsStore.fetch()
        }
        finally {
            loading.value = false
        }
    }
})

// ── Filtered destinations ───────────────────────────────────────

const availableDestinations = computed(() => {
    const all = destinationsStore.destinations
    if (!props.compatibleKeys?.length) return all
    return all.filter(d => props.compatibleKeys!.includes(d.key))
})

// ── Selection ───────────────────────────────────────────────────

function toggle(id: string) {
    const idx = selectedIds.value.indexOf(id)
    if (idx >= 0) selectedIds.value.splice(idx, 1)
    else selectedIds.value.push(id)
}

function isSelected(id: string) {
    return selectedIds.value.includes(id)
}

// ── Create new ──────────────────────────────────────────────────

async function handleCreated() {
    // Refresh destinations so the new one appears in the list.
    const before = new Set(destinationsStore.destinations.map(d => d.id))
    await destinationsStore.fetch()
    // Auto-select the newly created destination.
    for (const d of destinationsStore.destinations) {
        if (!before.has(d.id) && !selectedIds.value.includes(d.id)) {
            selectedIds.value.push(d.id)
        }
    }
    drawerOpen.value = false
}

function destIcon(key: string) {
    return componentIcon(key, 'i-lucide-hard-drive')
}

function destLabel(key: string) {
    const defn = catalogStore.getDestinationDefinition(key)
    return defn?.name ?? key
}
</script>

<template>
    <div class="flex flex-col gap-4">
        <!-- Header -->
        <div class="flex items-center justify-between">
            <p class="text-sm text-muted">
                Select where the data should be written.
            </p>
            <UButton size="xs"
                     variant="ghost"
                     icon="i-lucide-plus"
                     label="Create new"
                     @click="drawerOpen = true" />
        </div>

        <!-- Loading -->
        <div v-if="loading"
             class="flex items-center justify-center py-8">
            <UIcon name="i-lucide-loader-circle"
                   class="size-5 animate-spin text-muted" />
        </div>

        <template v-else>
            <!-- Existing destinations -->
            <div v-if="availableDestinations.length > 0"
                 class="flex flex-col gap-2.5">
                <SelectionCard v-for="dest in availableDestinations"
                               :key="dest.id"
                               :selected="isSelected(dest.id)"
                               class="flex items-center gap-3 px-4 py-3"
                               @select="toggle(dest.id)">
                    <UCheckbox :model-value="isSelected(dest.id)"
                               @click.stop
                               @update:model-value="toggle(dest.id)" />
                    <div class="size-10 shrink-0 rounded-[11px] border border-default bg-default flex items-center justify-center">
                        <UIcon :name="destIcon(dest.key)"
                               class="size-6" />
                    </div>
                    <span class="text-[14.5px] font-semibold text-highlighted truncate">{{ dest.name || destLabel(dest.key) }}</span>
                </SelectionCard>
            </div>

            <!-- Empty state -->
            <InlineEmptyState v-if="availableDestinations.length === 0"
                              icon="i-lucide-hard-drive"
                              message="No destinations yet."
                              action-label="Create destination"
                              @action="drawerOpen = true" />
        </template>

        <!-- Nested drawer for destination creation -->
        <UDrawer v-model:open="drawerOpen"
                 direction="right"
                 nested
                 :handle="false"
                 :handle-only="true"
                 :title="destStepperRef?.title ?? 'New Destination'"
                 :ui="{ content: 'w-[36rem]', description: 'sr-only' }">
            <template #description>Create a destination</template>
            <template #body>
                <DestinationsStepper v-if="drawerOpen"
                                      ref="destStepperRef"
                                      mode="standalone"
                                      :compatible-keys="props.compatibleKeys ?? []"
                                      @created="handleCreated" />
            </template>
            <template #footer>
                <StepperNav v-if="destStepperRef"
                            :can-proceed="destStepperRef.canProceed"
                            :has-prev="destStepperRef.hasPrev"
                            :submitting="destStepperRef.submitting"
                            :submit-label="destStepperRef.submitLabel"
                            @next="destStepperRef.next()"
                            @prev="destStepperRef.prev()" />
            </template>
        </UDrawer>
    </div>
</template>

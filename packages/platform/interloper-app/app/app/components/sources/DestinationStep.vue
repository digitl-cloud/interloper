<script setup lang="ts">
import type { ComponentRecord } from '~/types/component'

/**
 * Step for selecting destinations for a source.
 *
 * Behaves as a multiselect: lists all existing destinations, user toggles
 * which ones to attach. "New destination" opens a nested drawer with the
 * destination wizard, which creates the destination right away; it then
 * joins the list, selected.
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
const componentsStore = useComponentsStore()
const appConfig = useAppConfig()

const drawerOpen = ref(false)
const destStepperRef = ref<any>(null)
const loading = ref(false)

const destinations = computed(() => componentsStore.byKind('destination'))

/** Type choices for the nested create drawer, filtered to compatible keys. */
const compatibleDefinitions = computed(() => {
    const all = catalogStore.destinationDefinitions
    if (!props.compatibleKeys?.length) return all
    return all.filter(d => props.compatibleKeys!.includes(d.key))
})

// ── Load destinations ───────────────────────────────────────────

onMounted(async () => {
    loading.value = true
    try {
        await componentsStore.fetchAll(['destination'])
    }
    finally {
        loading.value = false
    }
})

// ── Filtered destinations ───────────────────────────────────────

const availableDestinations = computed(() => {
    const all = destinations.value
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

/** Select the destination just created; the store already lists it, from the create's response. */
async function selectCreated(created: ComponentRecord) {
    if (!selectedIds.value.includes(created.id)) selectedIds.value.push(created.id)
}

function destLabel(key: string) {
    const defn = catalogStore.getDestinationDefinition(key)
    return defn?.name ?? key
}
</script>

<template>
    <div class="flex flex-col gap-6">
        <!-- Header -->
        <div class="flex items-center justify-between gap-2">
            <p class="text-sm text-muted">
                Select where the data should be written.
            </p>
            <UButton size="sm"
                     color="neutral"
                     variant="outline"
                     :icon="appConfig.ui.icons.plus"
                     label="New destination"
                     @click="drawerOpen = true" />
        </div>

        <!-- Loading -->
        <div v-if="loading"
             class="flex items-center justify-center py-8">
            <UIcon :name="appConfig.ui.icons.loading"
                   class="size-5 animate-spin text-dimmed" />
        </div>

        <template v-else>
            <!-- Existing destinations -->
            <div v-if="availableDestinations.length > 0"
                 class="flex flex-col gap-2">
                <SelectionCard v-for="dest in availableDestinations"
                               :key="dest.id"
                               :selected="isSelected(dest.id)"
                               :label="dest.name || destLabel(dest.key)"
                               :icon="componentIcon(dest.key, appConfig.icons.destination)"
                               @select="toggle(dest.id)">
                    <template #leading>
                        <UCheckbox :model-value="isSelected(dest.id)"
                                   @click.stop
                                   @update:model-value="toggle(dest.id)" />
                    </template>
                </SelectionCard>
            </div>

            <UEmpty v-else
                    variant="subtle"
                    size="sm"
                    :icon="appConfig.icons.destination"
                    title="No destinations yet"
                    :actions="[{ label: 'New destination', icon: appConfig.ui.icons.plus, onClick: () => { drawerOpen = true } }]" />
        </template>

        <!-- Nested drawer for destination creation -->
        <UDrawer v-model:open="drawerOpen"
                 direction="right"
                 nested
                 :handle="false"
                 :handle-only="true"
                 close
                 :title="destStepperRef?.title ?? 'New destination'"
                 :ui="{ description: 'sr-only' }">
            <template #description>Create a destination</template>
            <template #body>
                <WizardDefinitionStepper v-if="drawerOpen"
                                         ref="destStepperRef"
                                         kind="destination"
                                         noun="Destination"
                                         :component="null"
                                         :definitions="compatibleDefinitions"
                                         name-optional
                                         :after-save="selectCreated"
                                         @created="drawerOpen = false" />
            </template>
            <template #footer>
                <WizardStepperNav v-if="destStepperRef"
                                  :last-step="destStepperRef.isLastStep"
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

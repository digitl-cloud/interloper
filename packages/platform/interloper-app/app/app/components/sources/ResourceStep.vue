<script setup lang="ts">
/**
 * Step for selecting or creating a resource instance.
 *
 * Lists existing instances matching the resource key. User picks one or
 * opens a nested drawer to create a new instance.
 *
 * Fetches its own matching instances on mount — no parent pre-fetch needed.
 */
import type { FormError } from '@nuxt/ui'
import type { ComponentDefinition } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'

const selectedId = defineModel<string>({ default: '' })

const props = withDefaults(defineProps<{
    /** The relation name (e.g. "connection"). */
    relationName: string
    /** The resource definition from the catalog. */
    definition: ComponentDefinition
    /**
     * Resource data from sibling steps, keyed by relation name.
     * Passed through to SchemaForm for x-fetch field resolution.
     */
    resourceContext?: Record<string, Record<string, unknown>> | undefined
    /** Suppress success toasts (e.g. when inside a collect-mode wizard). */
    silent?: boolean
}>(), {
    resourceContext: undefined,
    silent: false,
})

const emit = defineEmits<{
    created: [resource: ComponentRecord]
}>()

const componentsStore = useComponentsStore()
const toast = useToast()
const appConfig = useAppConfig()

const drawerOpen = ref(false)
const newName = ref(props.definition.name ?? '')
const formData = ref<Record<string, any>>({})
const formValid = ref(false)
/** Whether the schema form is in manual credential entry (vs OAuth sign-in). */
const manualCreds = ref(true)

const schemaForm = useTemplateRef('schemaForm')

/** Surface the check's static-validation errors under the form fields. */
function applyCheckErrors(errors: FormError[]) {
    schemaForm.value?.setErrors(errors)
}
const creating = ref(false)
const loadingInstances = ref(false)

const schema = computed(() => (props.definition as any).config_schema ?? null)

/** The relation as a lowercase noun for titles and buttons: "connection". */
const noun = computed(() => props.relationName.toLowerCase())

// ── Instance loading ─────────────────────────────────────────────

/** Matching resource instances for this relation's key. */
const instances = computed(() =>
    componentsStore.byKind(props.definition.kind).filter(r => r.key === props.definition.key),
)

/** Fetch resources if the store has none of this kind yet, then auto-select. */
onMounted(async () => {
    if (instances.value.length === 0 && !componentsStore.loading) {
        loadingInstances.value = true
        try {
            await componentsStore.fetchAll([props.definition.kind])
        }
        finally {
            loadingInstances.value = false
        }
    }
    autoSelectSingle()
})

/** Auto-select when instances change (e.g. after fetch or after creating one). */
watch(instances, () => autoSelectSingle())

function autoSelectSingle() {
    if (selectedId.value) return
    if (instances.value.length === 1 && instances.value[0]) {
        selectedId.value = instances.value[0].id
    }
}

// ── Create ───────────────────────────────────────────────────────

const canCreate = computed(() =>
    !!newName.value.trim() && (formValid.value || !schema.value),
)

function openCreateDrawer() {
    newName.value = props.definition.name ?? ''
    formData.value = {}
    formValid.value = false
    drawerOpen.value = true
}

async function handleCreate() {
    if (!newName.value.trim()) return
    creating.value = true
    try {
        const resource = await componentsStore.create({
            kind: props.definition.kind,
            key: props.definition.key,
            name: newName.value.trim(),
            config: formData.value,
        })
        selectedId.value = resource.id
        drawerOpen.value = false
        emit('created', resource)
        if (!props.silent) toast.add({ title: `${props.definition.name} created`, color: 'success' })
    }
    catch (e) {
        toast.add(errorToast(e, `Couldn't create ${noun.value}`))
    }
    finally {
        creating.value = false
    }
}

defineExpose({ formData })
</script>

<template>
    <!-- ── Loading ─────────────────────────────────────────────── -->
    <div v-if="loadingInstances"
         class="flex items-center justify-center py-8">
        <UIcon :name="appConfig.ui.icons.loading"
               class="size-5 animate-spin text-dimmed" />
    </div>

    <!-- ── Select or create ─────────────────────────────────── -->
    <div v-else
         class="flex flex-col gap-4">
        <!-- Header -->
        <div class="flex items-center justify-between gap-2">
            <p class="text-sm text-muted">
                Select an existing <span class="font-medium text-highlighted">{{ definition.name }}</span> {{ noun }} or create one.
            </p>
            <UButton size="sm"
                     color="neutral"
                     variant="outline"
                     :icon="appConfig.ui.icons.plus"
                     :label="`New ${noun}`"
                     @click="openCreateDrawer" />
        </div>

        <UEmpty v-if="instances.length === 0"
                variant="subtle"
                size="sm"
                :icon="componentIcon(definition.key)"
                :title="`No ${definition.name} ${noun} yet`"
                :actions="[{ label: `New ${noun}`, icon: appConfig.ui.icons.plus, onClick: openCreateDrawer }]" />

        <!-- Instance list -->
        <div v-else
             class="flex flex-col gap-2">
            <SelectionCard v-for="inst in instances"
                           :key="inst.id"
                           :selected="selectedId === inst.id"
                           :label="inst.name ?? definition.name"
                           :icon="componentIcon(definition.key)"
                           @select="selectedId = inst.id">
                <template v-if="selectedId === inst.id"
                          #trailing>
                    <UIcon :name="appConfig.ui.icons.check"
                           class="size-4 shrink-0 text-primary" />
                </template>
            </SelectionCard>
        </div>

        <!-- Create drawer (nested) -->
        <UDrawer v-model:open="drawerOpen"
                 direction="right"
                 nested
                 :handle="false"
                 :handle-only="true"
                 close
                 :title="`New ${noun}`"
                 :ui="{ description: 'sr-only' }">
            <template #description>
                Create a new {{ definition.name }} {{ noun }}
            </template>

            <template #body>
                <div class="flex flex-col gap-4">
                    <div class="flex items-center gap-2 text-sm text-muted">
                        <UIcon :name="componentIcon(definition.key)"
                               class="size-4" />
                        <span>{{ definition.name }}</span>
                    </div>

                    <UFormField label="Name"
                                required>
                        <UInput v-model="newName"
                                :placeholder="definition.name"
                                class="w-full" />
                    </UFormField>

                    <USeparator v-if="schema" />

                    <SchemaForm v-if="schema"
                                ref="schemaForm"
                                v-model:data="formData"
                                v-model:is-valid="formValid"
                                v-model:manual-mode="manualCreds"
                                :schema="schema"
                                :resource-context="resourceContext" />

                    <ResourcesConnectionCheck v-if="definition.checkable && manualCreds"
                                              :component-key="definition.key"
                                              :config="formData"
                                              manual
                                              @field-errors="applyCheckErrors" />
                </div>
            </template>

            <template #footer>
                <div class="flex w-full justify-between gap-2">
                    <UButton label="Cancel"
                             color="neutral"
                             variant="outline"
                             @click="drawerOpen = false" />
                    <UButton :disabled="!canCreate || creating"
                             :loading="creating"
                             :label="`Create ${noun}`"
                             @click="handleCreate" />
                </div>
            </template>
        </UDrawer>
    </div>
</template>

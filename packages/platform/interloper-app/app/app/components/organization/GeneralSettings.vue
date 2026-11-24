<script setup lang="ts">
/**
 * An organisation's display name and key, as settings rows in a card under a
 * section header, saved together by the header's "Save changes", as the
 * profile is. Rows the page adds about the same organisation go in the default
 * slot, as more horizontal `UFormField`s; a separator comes before them.
 */
import type { Organisation } from '~/types/organisation'

const props = defineProps<{
    organisation: Pick<Organisation, 'id' | 'name' | 'key'>
    /** The section header's title. */
    title: string
    /** The section header's description. */
    description: string
}>()

const emit = defineEmits<{
    saved: [organisation: Organisation]
}>()

const organisationStore = useOrganisationStore()
const toast = useToast()
const origin = window.location.host
const formId = useId()

const name = ref(props.organisation.name)
const key = ref(props.organisation.key)
const nameError = ref<string | null>(null)
const keyError = ref<string | null>(null)
const saving = ref(false)

const nameChanged = computed(() => name.value.trim() !== props.organisation.name)
const keyChanged = computed(() => key.value.trim() !== props.organisation.key)
const canSave = computed(() => (nameChanged.value || keyChanged.value) && !!name.value.trim() && !!key.value.trim())

watch(() => props.organisation, (organisation) => {
    name.value = organisation.name
    key.value = organisation.key
})
watch(name, () => {
    nameError.value = null
})
watch(key, () => {
    keyError.value = null
})

async function save() {
    if (!canSave.value || saving.value) return
    const patch = {
        ...(nameChanged.value ? { name: name.value.trim() } : {}),
        ...(keyChanged.value ? { key: key.value.trim() } : {}),
    }
    saving.value = true
    try {
        const updated = await organisationStore.updateOrganisation(props.organisation.id, patch)
        toast.add({ title: 'Organisation saved', color: 'success' })
        emit('saved', updated)
    }
    catch (err) {
        // The API refuses only a key (taken, malformed or reserved); a name is refused only empty, which never leaves.
        const detail = errorDetail(err) ?? GENERIC_ERROR
        if (patch.key) keyError.value = detail
        else nameError.value = detail
    }
    finally {
        saving.value = false
    }
}
</script>

<template>
    <div class="flex flex-col gap-4">
        <UPageCard :title="title"
                   :description="description"
                   variant="naked"
                   orientation="horizontal">
            <UButton :form="formId"
                     type="submit"
                     label="Save changes"
                     class="w-fit lg:ms-auto"
                     :disabled="!canSave"
                     :loading="saving" />
        </UPageCard>

        <UCard>
            <form :id="formId"
                  class="flex flex-col gap-4"
                  @submit.prevent="save">
                <UFormField label="Display name"
                            description="Members see this name everywhere in the app."
                            :error="nameError ?? undefined"
                            required
                            orientation="horizontal">
                    <UInput v-model="name"
                            class="w-60"
                            :disabled="saving" />
                </UFormField>

                <USeparator />

                <UFormField label="Key"
                            :error="keyError ?? undefined"
                            required
                            orientation="horizontal">
                    <template #description>
                        Identifies the organisation in the app's addresses, as in
                        <span class="text-default">{{ origin }}/{{ organisation.key }}</span>.
                        Links with the old key stop working once it changes.
                    </template>
                    <UInput v-model="key"
                            class="w-60"
                            :disabled="saving" />
                </UFormField>

                <template v-if="$slots.default">
                    <USeparator />
                    <slot />
                </template>
            </form>
        </UCard>
    </div>
</template>

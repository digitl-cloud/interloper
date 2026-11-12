<script setup lang="ts">
/**
 * An organisation's display name and key, each saved on its own, as settings
 * rows in a card. Given a `title`, the section header above the card comes
 * with it. Rows the page adds about the same organisation go in the default
 * slot, as more horizontal `UFormField`s; a separator comes before them.
 */
import type { Organisation } from '~/types/organisation'

const props = withDefaults(defineProps<{
    organisation: Pick<Organisation, 'id' | 'name' | 'key'>
    /** The section header's title; without it, the page heads the card itself. */
    title?: string
    /** The section header's description. */
    description?: string
}>(), { title: undefined, description: undefined })

const emit = defineEmits<{
    saved: [organisation: Organisation]
}>()

const organisationStore = useOrganisationStore()
const toast = useToast()
const origin = window.location.host

const name = ref(props.organisation.name)
const nameError = ref<string | null>(null)
const savingName = ref(false)
const nameChanged = computed(() => name.value.trim() !== props.organisation.name)

const key = ref(props.organisation.key)
const keyError = ref<string | null>(null)
const savingKey = ref(false)
const keyChanged = computed(() => key.value.trim() !== props.organisation.key)

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

async function saveName() {
    const next = name.value.trim()
    if (!next || !nameChanged.value) return
    savingName.value = true
    try {
        const updated = await organisationStore.updateOrganisation(props.organisation.id, { name: next })
        toast.add({ title: 'Organisation renamed', color: 'success' })
        emit('saved', updated)
    }
    catch (err) {
        nameError.value = errorDetail(err) ?? GENERIC_ERROR
    }
    finally {
        savingName.value = false
    }
}

async function saveKey() {
    const next = key.value.trim()
    if (!next || !keyChanged.value) return
    savingKey.value = true
    try {
        const updated = await organisationStore.updateOrganisation(props.organisation.id, { key: next })
        toast.add({ title: `Organisation key changed to ${updated.key}`, color: 'success' })
        emit('saved', updated)
    }
    catch (err) {
        keyError.value = errorDetail(err) ?? GENERIC_ERROR
    }
    finally {
        savingKey.value = false
    }
}
</script>

<template>
    <div class="flex flex-col gap-4">
        <UPageCard v-if="title"
                   :title="title"
                   :description="description"
                   variant="naked"
                   orientation="horizontal" />

        <UCard>
            <div class="flex flex-col gap-4">
                <form @submit.prevent="saveName">
                    <UFormField label="Display name"
                                description="Members see this name everywhere in the app."
                                :error="nameError ?? undefined"
                                required
                                orientation="horizontal">
                        <div class="flex items-center gap-2">
                            <UInput v-model="name"
                                    class="w-60"
                                    :disabled="savingName" />
                            <UButton type="submit"
                                     label="Save"
                                     :disabled="!name.trim() || !nameChanged || savingName"
                                     :loading="savingName" />
                        </div>
                    </UFormField>
                </form>

                <USeparator />

                <form @submit.prevent="saveKey">
                    <UFormField label="Key"
                                :error="keyError ?? undefined"
                                required
                                orientation="horizontal">
                        <template #description>
                            Identifies the organisation in the app's addresses, as in
                            <span class="text-default">{{ origin }}/{{ organisation.key }}</span>.
                            Links with the old key stop working once it changes.
                        </template>
                        <div class="flex items-center gap-2">
                            <UInput v-model="key"
                                    class="w-60"
                                    :disabled="savingKey" />
                            <UButton type="submit"
                                     label="Save"
                                     :disabled="!key.trim() || !keyChanged || savingKey"
                                     :loading="savingKey" />
                        </div>
                    </UFormField>
                </form>

                <template v-if="$slots.default">
                    <USeparator />
                    <slot />
                </template>
            </div>
        </UCard>
    </div>
</template>

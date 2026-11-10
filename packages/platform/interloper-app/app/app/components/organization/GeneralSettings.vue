<script setup lang="ts">
/**
 * An organisation's display name and key, each saved on its own. Rows the
 * page adds about the same organisation go in the default slot.
 */
import type { Organisation } from '~/types/organisation'

const props = defineProps<{
    organisation: Pick<Organisation, 'id' | 'name' | 'key'>
}>()

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
    <UCard :ui="{ body: 'p-0 sm:p-0' }">
        <div class="divide-y divide-default">
            <form class="flex items-start gap-4 px-4 py-4 sm:px-6"
                  @submit.prevent="saveName">
                <div class="min-w-0 flex-1">
                    <div class="text-sm font-medium text-highlighted">Display name</div>
                    <div class="mt-0.5 text-sm text-muted">Members see this name everywhere in the app.</div>
                </div>
                <UFormField :error="nameError ?? undefined"
                            class="w-60 max-w-[50%]">
                    <UInput v-model="name"
                            class="w-full"
                            :disabled="savingName" />
                </UFormField>
                <UButton type="submit"
                         label="Save"
                         :disabled="!name.trim() || !nameChanged || savingName"
                         :loading="savingName" />
            </form>
            <form class="flex items-start gap-4 px-4 py-4 sm:px-6"
                  @submit.prevent="saveKey">
                <div class="min-w-0 flex-1">
                    <div class="text-sm font-medium text-highlighted">Key</div>
                    <div class="mt-0.5 text-sm text-muted">
                        Identifies the organisation in the app's addresses, as in
                        <span class="font-mono text-default">{{ origin }}/{{ organisation.key }}</span>.
                        Links with the old key stop working once it changes.
                    </div>
                </div>
                <UFormField :error="keyError ?? undefined"
                            class="w-60 max-w-[50%]">
                    <UInput v-model="key"
                            class="w-full"
                            :ui="{ base: 'font-mono' }"
                            :disabled="savingKey" />
                </UFormField>
                <UButton type="submit"
                         label="Save"
                         :disabled="!key.trim() || !keyChanged || savingKey"
                         :loading="savingKey" />
            </form>
            <slot />
        </div>
    </UCard>
</template>

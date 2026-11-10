<script setup lang="ts">
import type { CreatedOAuthClient } from '~/types/admin'

const open = defineModel<boolean>('open', { default: false })

const emit = defineEmits<{
    created: []
}>()

const adminStore = useAdminStore()
const toast = useToast()

const name = ref('')
const redirectUris = ref<string[]>([''])
const submitting = ref(false)
const created = ref<CreatedOAuthClient | null>(null)
const copied = ref<'id' | 'secret' | null>(null)

watch(open, (isOpen) => {
    if (!isOpen) {
        name.value = ''
        redirectUris.value = ['']
        created.value = null
        copied.value = null
    }
})

const filledUris = computed(() => redirectUris.value.map(uri => uri.trim()).filter(Boolean))
const canSubmit = computed(() => name.value.trim().length > 0 && filledUris.value.length > 0 && !submitting.value)

function addUri() {
    redirectUris.value.push('')
}

function removeUri(index: number) {
    redirectUris.value.splice(index, 1)
}

async function submit() {
    if (!canSubmit.value) return
    submitting.value = true
    try {
        created.value = await adminStore.createOAuthClient(name.value.trim(), filledUris.value)
        emit('created')
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to register client'))
    }
    finally {
        submitting.value = false
    }
}

async function copy(field: 'id' | 'secret') {
    if (!created.value) return
    await navigator.clipboard.writeText(field === 'id' ? created.value.id : created.value.client_secret)
    copied.value = field
}
</script>

<template>
    <UModal v-model:open="open"
            title="New OAuth client"
            description="A confidential client for an app that cannot register itself, such as a Slack app."
            :dismissible="!created"
            :ui="{ footer: 'justify-end' }">
        <template #body>
            <div v-if="!created"
                 class="flex flex-col gap-4">
                <UFormField label="Name"
                            help="Shown to users on the consent page and in their authorised apps.">
                    <UInput v-model="name"
                            placeholder="Client name"
                            class="w-full"
                            autofocus />
                </UFormField>
                <UFormField label="Redirect URIs"
                            help="Exact URIs a code may be sent to: https, http on a loopback host, or a known app scheme.">
                    <div class="flex flex-col gap-2">
                        <div v-for="(_, index) in redirectUris"
                             :key="index"
                             class="flex items-center gap-2">
                            <UInput v-model="redirectUris[index]"
                                    placeholder="https://example.com/oauth/callback"
                                    class="min-w-0 flex-1"
                                    :ui="{ base: 'font-mono text-[13px]' }"
                                    @keydown.enter="submit" />
                            <UButton v-if="redirectUris.length > 1"
                                     icon="i-lucide-x"
                                     color="neutral"
                                     variant="ghost"
                                     aria-label="Remove redirect URI"
                                     @click="removeUri(index)" />
                        </div>
                        <UButton label="Add redirect URI"
                                 icon="i-lucide-plus"
                                 color="neutral"
                                 variant="ghost"
                                 size="sm"
                                 class="self-start"
                                 @click="addUri" />
                    </div>
                </UFormField>
            </div>

            <div v-else
                 class="flex flex-col gap-4">
                <UAlert color="warning"
                        variant="subtle"
                        icon="i-lucide-triangle-alert"
                        title="Copy the client secret now"
                        description="It is shown only once and cannot be shown again. If it is lost, delete the client and register a new one." />
                <UFormField label="Client ID">
                    <div class="flex items-center gap-2">
                        <UInput :model-value="created.id"
                                readonly
                                class="min-w-0 flex-1"
                                :ui="{ base: 'font-mono text-[13px]' }" />
                        <UButton :icon="copied === 'id' ? 'i-lucide-check' : 'i-lucide-copy'"
                                 :label="copied === 'id' ? 'Copied' : 'Copy'"
                                 color="neutral"
                                 variant="outline"
                                 @click="copy('id')" />
                    </div>
                </UFormField>
                <UFormField label="Client secret">
                    <div class="flex items-center gap-2">
                        <UInput :model-value="created.client_secret"
                                readonly
                                class="min-w-0 flex-1"
                                :ui="{ base: 'font-mono text-[13px]' }" />
                        <UButton :icon="copied === 'secret' ? 'i-lucide-check' : 'i-lucide-copy'"
                                 :label="copied === 'secret' ? 'Copied' : 'Copy'"
                                 color="neutral"
                                 variant="outline"
                                 @click="copy('secret')" />
                    </div>
                </UFormField>
            </div>
        </template>

        <template #footer>
            <template v-if="!created">
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="open = false" />
                <UButton label="Register client"
                         icon="i-lucide-key-round"
                         :disabled="!canSubmit"
                         :loading="submitting"
                         @click="submit" />
            </template>
            <UButton v-else
                     label="Done"
                     @click="open = false" />
        </template>
    </UModal>
</template>

<script setup lang="ts">
/**
 * Registers a confidential OAuth client, then shows its id and secret once:
 * from then on the modal closes only through its own buttons, so the secret
 * can't be lost to a stray click outside.
 */
import type { CreatedOAuthClient } from '~/types/admin'

const open = defineModel<boolean>('open', { default: false })

const emit = defineEmits<{
    created: []
}>()

const adminStore = useAdminStore()
const toast = useToast()
const appConfig = useAppConfig()

const name = ref('')
const redirectUris = ref<string[]>([''])
const submitting = ref(false)
const created = ref<CreatedOAuthClient | null>(null)

watch(open, (isOpen) => {
    if (!isOpen) {
        name.value = ''
        redirectUris.value = ['']
        created.value = null
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
        toast.add(errorToast(err, "Couldn't register client"))
    }
    finally {
        submitting.value = false
    }
}
</script>

<template>
    <UModal v-model:open="open"
            title="New OAuth client"
            description="A confidential client for an app that cannot register itself, such as a Slack app."
            :dismissible="!created">
        <template #body>
            <div v-if="!created"
                 class="flex flex-col gap-4">
                <UFormField label="Name"
                            description="Shown to users on the consent page and in their authorised apps."
                            required>
                    <UInput v-model="name"
                            placeholder="Client name"
                            class="w-full"
                            autofocus />
                </UFormField>
                <UFormField label="Redirect URIs"
                            description="Exact URIs a code may be sent to: https, http on a loopback host, or a known app scheme."
                            required>
                    <div class="flex flex-col gap-2">
                        <div v-for="(_, index) in redirectUris"
                             :key="index"
                             class="flex items-center gap-2">
                            <UInput v-model="redirectUris[index]"
                                    placeholder="https://example.com/oauth/callback"
                                    class="min-w-0 flex-1"
                                    @keydown.enter="submit" />
                            <UButton v-if="redirectUris.length > 1"
                                     :icon="appConfig.ui.icons.close"
                                     color="neutral"
                                     variant="ghost"
                                     aria-label="Remove redirect URI"
                                     @click="removeUri(index)" />
                        </div>
                        <UButton label="Add redirect URI"
                                 :icon="appConfig.ui.icons.plus"
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
                        :icon="appConfig.ui.icons.warning"
                        title="Copy the client secret now"
                        description="It is shown only once and cannot be shown again. If it is lost, delete the client and register a new one." />
                <UFormField label="Client ID">
                    <CopyField :value="created.id" />
                </UFormField>
                <UFormField label="Client secret">
                    <CopyField :value="created.client_secret" />
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
                         :icon="appConfig.icons.app"
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

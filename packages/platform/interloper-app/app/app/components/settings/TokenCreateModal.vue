<script setup lang="ts">
/**
 * Creates a personal token, then shows it once: from then on the modal
 * closes only through its own buttons, so the token can't be lost to a
 * stray click outside.
 */
import type { CreatedToken } from '~/types/token'

const open = defineModel<boolean>('open', { default: false })

const { apiFetch } = useApi()
const toast = useToast()
const appConfig = useAppConfig()

const emit = defineEmits<{
    created: []
}>()

const name = ref('')
const expiry = ref('90')
const submitting = ref(false)
const created = ref<CreatedToken | null>(null)

const expiryOptions = [
    { label: '30 days', value: '30' },
    { label: '90 days', value: '90' },
    { label: '1 year', value: '365' },
    { label: 'No expiry', value: 'never' },
]

watch(open, (isOpen) => {
    if (!isOpen) {
        name.value = ''
        expiry.value = '90'
        created.value = null
    }
})

const canSubmit = computed(() => name.value.trim().length > 0 && !submitting.value)

async function submit() {
    if (!canSubmit.value) return
    submitting.value = true
    try {
        created.value = await apiFetch<CreatedToken>('/auth/tokens', {
            method: 'POST',
            body: {
                name: name.value.trim(),
                expires_in_days: expiry.value === 'never' ? null : Number(expiry.value),
            },
        })
        emit('created')
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't create token"))
    }
    finally {
        submitting.value = false
    }
}
</script>

<template>
    <UModal v-model:open="open"
            title="New token"
            description="A token acts as you in every organisation you belong to."
            :dismissible="!created">
        <template #body>
            <div v-if="!created"
                 class="flex flex-col gap-4">
                <UFormField label="Name"
                            description="What this token is for, such as “MCP on my laptop”."
                            required>
                    <UInput v-model="name"
                            placeholder="Token name"
                            class="w-full"
                            autofocus
                            @keydown.enter="submit" />
                </UFormField>
                <UFormField label="Expiration"
                            required>
                    <USelect v-model="expiry"
                             :items="expiryOptions"
                             value-key="value"
                             class="w-full" />
                </UFormField>
            </div>

            <div v-else
                 class="flex flex-col gap-4">
                <UAlert color="warning"
                        :icon="appConfig.ui.icons.warning"
                        title="Copy your token now"
                        description="It is shown only once and cannot be recovered later." />
                <UFormField label="Token">
                    <CopyField :value="created.token" />
                </UFormField>
            </div>
        </template>

        <template #footer>
            <template v-if="!created">
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="open = false" />
                <UButton label="Create token"
                         :icon="appConfig.icons.token"
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

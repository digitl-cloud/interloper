<script setup lang="ts">
const open = defineModel<boolean>('open', { default: false })

const props = withDefaults(defineProps<{
    /** API path that accepts `{ email, role }` POST bodies. */
    endpoint?: string
}>(), {
    endpoint: '/organisations/invite',
})

const { apiFetch } = useApi()
const toast = useToast()

interface InviteRow {
    email: string
    role: string
}

const rows = ref<InviteRow[]>([{ email: '', role: 'viewer' }])
const submitting = ref(false)

const roleOptions = [
    { label: 'Viewer', value: 'viewer' },
    { label: 'Editor', value: 'editor' },
    { label: 'Admin', value: 'admin' },
]

const emit = defineEmits<{
    invited: []
}>()

const canSubmit = computed(() => rows.value.some(r => r.email.trim()))

watch(open, (isOpen) => {
    if (!isOpen) {
        rows.value = [{ email: '', role: 'viewer' }]
    }
})

function addRow() {
    rows.value.push({ email: '', role: 'viewer' })
}

function removeRow(index: number) {
    rows.value.splice(index, 1)
}

async function submit() {
    const invites = rows.value.filter(r => r.email.trim())
    if (invites.length === 0) return

    submitting.value = true
    let successCount = 0
    const errors: string[] = []

    for (const invite of invites) {
        try {
            await apiFetch(props.endpoint, {
                method: 'POST',
                body: { email: invite.email.trim(), role: invite.role },
            })
            successCount++
        }
        catch (err: any) {
            const detail = err?.data?.detail || 'Failed to send invitation'
            errors.push(`${invite.email}: ${detail}`)
        }
    }

    if (successCount > 0) {
        const label = successCount === 1 ? 'invitation' : 'invitations'
        toast.add({ title: `${successCount} ${label} sent`, color: 'success' })
        emit('invited')
    }
    for (const error of errors) {
        toast.add({ title: error, color: 'error' })
    }

    submitting.value = false
    if (errors.length === 0) {
        open.value = false
    }
}
</script>

<template>
    <UModal v-model:open="open"
            title="Invite Members"
            :ui="{ footer: 'justify-end' }">
        <template #body>
            <div class="flex flex-col gap-3 w-full">
                <div v-for="(row, index) in rows"
                     :key="index"
                     class="flex w-full items-center gap-2">
                    <UInput v-model="row.email"
                            type="email"
                            placeholder="user@example.com"
                            icon="i-lucide-mail"
                            class="min-w-0 flex-1"
                            :autofocus="index === 0" />
                    <USelect v-model="row.role"
                             :items="roleOptions"
                             value-key="value"
                             class="w-32 shrink-0" />
                    <UButton v-if="rows.length > 1"
                             icon="i-lucide-x"
                             color="neutral"
                             variant="ghost"
                             size="sm"
                             square
                             class="shrink-0"
                             @click="removeRow(index)" />
                </div>

                <UButton icon="i-lucide-plus"
                         label="Add another"
                         color="neutral"
                         variant="ghost"
                         size="sm"
                         class="self-start"
                         @click="addRow" />
            </div>
        </template>

        <template #footer>
            <UButton label="Cancel"
                     color="neutral"
                     variant="outline"
                     @click="open = false" />
            <UButton label="Send Invitations"
                     icon="i-lucide-send"
                     :disabled="!canSubmit || submitting"
                     :loading="submitting"
                     @click="submit" />
        </template>
    </UModal>
</template>

<script setup lang="ts">
/**
 * Invites people to an organisation, one email and role per row. Each
 * invitation is sent on its own: the sent ones are counted in one toast, each
 * failure gets its own, and the modal stays open while any failed.
 */
const open = defineModel<boolean>('open', { default: false })

const props = defineProps<{
    orgId: string
}>()

const organisationStore = useOrganisationStore()
const toast = useToast()
const appConfig = useAppConfig()

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
    let failed = false

    for (const invite of invites) {
        const email = invite.email.trim()
        try {
            await organisationStore.inviteMember(props.orgId, email, invite.role)
            successCount++
        }
        catch (err) {
            failed = true
            toast.add(errorToast(err, `Couldn't invite ${email}`))
        }
    }

    if (successCount > 0) {
        const label = successCount === 1 ? 'invitation' : 'invitations'
        toast.add({ title: `${successCount} ${label} sent`, color: 'success' })
        emit('invited')
    }

    submitting.value = false
    if (!failed) {
        open.value = false
    }
}
</script>

<template>
    <UModal v-model:open="open"
            title="Invite members">
        <template #body>
            <UFormField label="Invitations"
                        description="Each person gets an email with a link to join, in the role you pick."
                        required>
                <div class="flex flex-col gap-2">
                    <div v-for="(row, index) in rows"
                         :key="index"
                         class="flex items-center gap-2">
                        <UInput v-model="row.email"
                                type="email"
                                placeholder="user@example.com"
                                icon="i-lucide-mail"
                                class="min-w-0 flex-1"
                                :autofocus="index === 0" />
                        <USelect v-model="row.role"
                                 :items="roleOptions"
                                 value-key="value"
                                 aria-label="Role"
                                 class="w-32 shrink-0" />
                        <UButton v-if="rows.length > 1"
                                 :icon="appConfig.ui.icons.close"
                                 color="neutral"
                                 variant="ghost"
                                 aria-label="Remove invitation"
                                 @click="removeRow(index)" />
                    </div>
                    <UButton label="Add another"
                             :icon="appConfig.ui.icons.plus"
                             color="neutral"
                             variant="ghost"
                             size="sm"
                             class="self-start"
                             @click="addRow" />
                </div>
            </UFormField>
        </template>

        <template #footer>
            <UButton label="Cancel"
                     color="neutral"
                     variant="outline"
                     @click="open = false" />
            <UButton label="Send invitations"
                     :icon="appConfig.icons.invite"
                     :disabled="!canSubmit || submitting"
                     :loading="submitting"
                     @click="submit" />
        </template>
    </UModal>
</template>

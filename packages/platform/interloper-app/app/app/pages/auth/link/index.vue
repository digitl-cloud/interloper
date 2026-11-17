<script setup lang="ts">
import type { LoginProvider } from '~/types/user'

definePageMeta({
    layout: false,
})

/** The link a login proposed when its address already had an account, and the ways to prove it. */
interface LinkProposal {
    email: string
    provider: LoginProvider | null
    methods: LoginProvider[]
    email_available: boolean
    destination: string
}

const { apiFetch } = useApi()
const appConfig = useAppConfig()
const userStore = useUserStore()
const providerIcon = useLoginProviderIcon()

const proposal = ref<LinkProposal | null>(null)
const status = ref<'loading' | 'ready' | 'emailing' | 'emailed' | 'missing'>('loading')
const signingIn = ref<string | null>(null)
const errorMessage = ref('')

const newAccount = computed(() => proposal.value?.provider?.label ?? 'this account')

onMounted(async () => {
    try {
        proposal.value = await apiFetch<LinkProposal>('/auth/link')
        status.value = 'ready'
    }
    catch {
        status.value = 'missing'
    }
})

function signIn(method: LoginProvider) {
    signingIn.value = method.name
    userStore.signIn(method.name, proposal.value?.destination)
}

async function emailLink() {
    status.value = 'emailing'
    errorMessage.value = ''
    try {
        await apiFetch('/auth/link/email', { method: 'POST' })
        status.value = 'emailed'
    }
    catch (err) {
        errorMessage.value = errorDetail(err) ?? GENERIC_ERROR
        status.value = 'ready'
    }
}

async function cancel() {
    await apiFetch('/auth/link', { method: 'DELETE' }).catch(() => {})
    await navigateTo('/login')
}
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6">
        <UCard class="w-full max-w-sm">
            <template #header>
                <div class="flex flex-col items-center gap-3 text-center">
                    <LogoIcon class="h-8 text-primary" />
                    <h1 class="text-2xl font-semibold text-highlighted">
                        Link your accounts
                    </h1>
                </div>
            </template>

            <div v-if="status === 'loading'"
                 class="flex justify-center py-6">
                <UIcon :name="appConfig.ui.icons.loading"
                       class="size-5 animate-spin text-dimmed" />
            </div>

            <div v-else-if="status === 'missing'"
                 class="flex flex-col gap-4">
                <UAlert color="warning"
                        :icon="appConfig.ui.icons.warning"
                        title="Nothing is waiting to be linked"
                        description="The request expired or was cancelled. Sign in again." />
                <UButton label="Sign in"
                         block
                         to="/login" />
            </div>

            <div v-else-if="status === 'emailed'"
                 class="flex flex-col gap-4">
                <UAlert color="info"
                        :icon="appConfig.ui.icons.info"
                        title="Check your inbox"
                        :description="`Follow the link we emailed to ${proposal?.email}, then sign in with ${newAccount}.`" />
                <UButton label="Back to sign in"
                         color="neutral"
                         variant="outline"
                         block
                         to="/login" />
            </div>

            <div v-else-if="proposal"
                 class="flex flex-col gap-4">
                <p class="text-sm text-muted">
                    An Interloper account already uses {{ proposal.email }}. To sign in to it with
                    {{ newAccount }} too, show it's yours:
                </p>

                <UAlert v-if="errorMessage"
                        color="error"
                        :icon="appConfig.ui.icons.error"
                        title="Couldn't send the link"
                        :description="errorMessage" />

                <UButton v-for="method in proposal.methods"
                         :key="method.name"
                         block
                         :icon="providerIcon(method.name)"
                         :label="`Sign in with ${method.label}`"
                         :loading="signingIn === method.name"
                         :disabled="signingIn !== null && signingIn !== method.name"
                         @click="signIn(method)" />

                <UButton v-if="proposal.email_available"
                         label="Email me a link instead"
                         color="neutral"
                         variant="outline"
                         block
                         :loading="status === 'emailing'"
                         @click="emailLink" />

                <UAlert v-if="!proposal.methods.length && !proposal.email_available"
                        color="warning"
                        :icon="appConfig.ui.icons.warning"
                        title="No way to prove it here. Ask an administrator." />

                <UButton label="Cancel"
                         color="neutral"
                         variant="ghost"
                         block
                         @click="cancel" />
            </div>
        </UCard>
    </div>
</template>

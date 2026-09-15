<script setup lang="ts">
/**
 * "Sign in with X" button that triggers the OAuth popup flow.
 *
 * When the flow completes, emits `success` with the token the provider issued
 * (refresh_token, or the access_token of long-lived-token providers). A
 * response carrying neither is a failure — the form field would otherwise stay
 * empty behind a "Connected" toast. The in-house app credentials are never
 * returned — connections resolve them from env at runtime — so a per-user
 * override of client_id/client_secret stays blank unless filled manually.
 */

const props = defineProps<{
    provider: OAuthProviderKey
    scope?: string
    connected?: boolean
}>()

const emit = defineEmits<{
    success: [token: string]
}>()

const catalogStore = useCatalogStore()
const { signIn } = useOAuthPopup()
const { getAuthUrl } = useOAuthProvider()
const toast = useToast()

const loading = ref(false)

const providerInfo = computed(() => catalogStore.getOAuthProvider(props.provider))
const available = computed(() => catalogStore.isOAuthProviderAvailable(props.provider))

async function handleSignIn() {
    const url = getAuthUrl(props.provider, props.scope)
    if (!url) {
        toast.add({ title: 'OAuth provider not configured', color: 'error' })
        return
    }

    loading.value = true
    try {
        const token = oauthToken(await signIn(url))
        if (token === undefined) throw new Error('The provider response carried no token')
        emit('success', token)
        toast.add({ title: `Connected to ${providerInfo.value?.label ?? props.provider}`, color: 'success' })
    }
    catch (error) {
        // Closing the popup is a deliberate cancel, not a failure.
        if (!(error instanceof OAuthCancelledError)) {
            toast.add(errorToast(error, 'Sign-in failed'))
        }
    }
    finally {
        loading.value = false
    }
}
</script>

<template>
    <UButton v-if="available && connected"
             :icon="'i-lucide-circle-check'"
             :label="`Connected to ${providerInfo?.label ?? provider}`"
             color="success"
             variant="subtle"
             block
             disabled />
    <UButton v-else-if="available"
             :icon="providerInfo?.icon"
             :label="`Sign in with ${providerInfo?.label ?? provider}`"
             :loading="loading"
             block
             @click="handleSignIn" />
</template>

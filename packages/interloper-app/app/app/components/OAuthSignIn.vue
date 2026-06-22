<script setup lang="ts">
/**
 * "Sign in with X" button that triggers the OAuth popup flow.
 *
 * When the flow completes, emits `success` with the provider's token
 * response (e.g. refresh_token). The in-house app credentials are never
 * returned — connections resolve them from env at runtime — so a per-user
 * override of client_id/client_secret stays blank unless filled manually.
 */

const props = defineProps<{
    provider: OAuthProviderKey
    scope?: string
    connected?: boolean
}>()

const emit = defineEmits<{
    success: [tokens: Record<string, unknown>]
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
        const tokens = await signIn(url)
        emit('success', tokens)
        toast.add({ title: `Connected to ${providerInfo.value?.label ?? props.provider}`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Sign-in failed', color: 'error' })
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

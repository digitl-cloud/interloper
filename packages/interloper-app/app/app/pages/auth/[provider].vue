<script setup lang="ts">
definePageMeta({
    title: 'Auth',
    layout: false,
    auth: false,
})

const route = useRoute()
const { exchangeCode } = useOAuthPopup()

const provider = route.params.provider?.toString()
if (!provider) {
    throw createError({
        statusCode: 404,
        statusMessage: 'Provider not found',
    })
}

const code = route.query.code as string | undefined
const status = ref<OAuthPopupStatus>(
    code ? OAuthPopupStatus.Loading : OAuthPopupStatus.MissingCode,
)

onMounted(async () => {
    if (code) {
        try {
            await exchangeCode(provider, code)
            status.value = OAuthPopupStatus.Success
        }
        catch {
            status.value = OAuthPopupStatus.Failure
        }
    }
})
</script>

<template>
    <div class="h-screen flex items-center justify-center p-8">
        <UAlert v-if="status === OAuthPopupStatus.Loading"
                color="info"
                variant="subtle"
                icon="i-lucide-loader-circle"
                title="Authenticating"
                :description="`Connecting to ${provider}...`" />
        <UAlert v-else-if="status === OAuthPopupStatus.Success"
                color="success"
                variant="subtle"
                icon="i-lucide-circle-check"
                :title="`Successfully connected to ${provider}`"
                description="You can close this window." />
        <UAlert v-else-if="status === OAuthPopupStatus.MissingCode"
                color="error"
                variant="subtle"
                icon="i-lucide-circle-x"
                title="Authentication Error"
                description="Authorization code is missing." />
        <UAlert v-else
                color="error"
                variant="subtle"
                icon="i-lucide-circle-x"
                title="Authentication Failed"
                description="Something went wrong during authentication. You can close this window." />
    </div>
</template>

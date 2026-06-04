import type { User } from '~/types/user'

export const useUserStore = defineStore('user', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const user = ref<User | null>(null)
    const loading = ref(false)
    const error = ref<Error | null>(null)
    const authenticated = computed(() => !!user.value)
    const isSuperAdmin = computed(() => !!user.value?.is_super_admin)

    /**********************
     * Actions
     **********************/
    async function fetchMe() {
        loading.value = true
        try {
            user.value = await apiFetch<User>('/auth/me')
        }
        catch {
            user.value = null
        }
        finally {
            loading.value = false
        }
    }

    function signIn(redirect?: string) {
        const params = redirect ? `?redirect=${encodeURIComponent(redirect)}` : ''
        window.location.href = `/api/auth/google${params}`
    }

    async function signOut() {
        await apiFetch('/auth/logout', { method: 'POST' })
        user.value = null
        return navigateTo('/login')
    }

    function findProfile(): User | null {
        return user.value
    }

    function requireProfile(): User {
        if (!user.value) {
            throw new Error('Profile not loaded')
        }
        return user.value
    }

    return {
        user,
        loading,
        error,
        authenticated,
        isSuperAdmin,
        findProfile,
        requireProfile,
        fetchMe,
        signIn,
        signOut,
    }
})

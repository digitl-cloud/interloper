import './main.css'
import { defineExtension } from '@interloper/host'

export default defineExtension({
    setup(host) {
        host.addRoute({ path: '', name: 'greeting', component: () => import('./pages/Greeting.vue') })
        host.addNavDestination({ label: 'Example', icon: 'i-lucide-flask-conical', path: '' })
    },
})

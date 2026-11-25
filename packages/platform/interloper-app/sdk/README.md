# @interloper/ui

Build an [interloper](https://docs.interloper.dev) extension's UI against the app it runs in:
Vue single-file components, the app's components used in templates without an import,
Tailwind classes that resolve to the app's theme, and the types of `@interloper/host`, the
module through which an extension reaches the app.

```sh
npm install --save-dev @interloper/ui vue
```

npm installs `vite`, `typescript` and `vue-tsc` with it, at the versions the SDK builds with.

An extension's UI needs four files beside its pages:

```ts
// vite.config.ts: the bundle goes into the extension's Python package
import { interloper } from '@interloper/ui/vite'

export default interloper({ outDir: '../src/interloper_acme/static' })
```

```json
// tsconfig.json
{ "extends": "@interloper/ui/tsconfig", "include": ["src"] }
```

```css
/* src/main.css */
@import "@interloper/ui/tailwind.css";
```

```ts
// src/index.ts
import './main.css'
import { defineExtension } from '@interloper/host'

export default defineExtension({
    setup(host) {
        host.addRoute({ path: '', name: 'home', component: () => import('./pages/Home.vue') })
        host.addNavDestination({ label: 'Acme', icon: 'i-lucide-box', path: '' })
    },
})
```

`vite build` writes the bundle and its manifest into `outDir`, where the extension's Python
package declares it with `UIBundle.from_vite`. The version of `@interloper/ui` is the
platform's: build against the version the deployment runs.

How an extension is put together, its routes and its UI:
[Extensions & registries](https://docs.interloper.dev/extending/extensions/#building-it-with-the-sdk).

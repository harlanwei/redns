import './app.css'
import { mount } from 'svelte'
import App from './App.svelte'
import { initTheme } from './lib/utils/theme.svelte'

initTheme()

const app = mount(App, {
    target: document.getElementById('app')!,
})

export default app

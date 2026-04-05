import { createApp } from 'vue'
import { createPinia } from 'pinia'
import { ElLoadingDirective } from 'element-plus'
import 'element-plus/es/components/loading/style/css'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import './styles/element/light.scss'
import './styles/element/dark.scss'
import App from './App.vue'
import router from './router'

const storedTheme = localStorage.getItem('amm_theme')
document.body.classList.toggle('theme-dark', storedTheme === 'dark')

const app = createApp(App)
const pinia = createPinia()

app.use(pinia)
app.use(router)
app.directive('loading', ElLoadingDirective)
app.mount('#app')

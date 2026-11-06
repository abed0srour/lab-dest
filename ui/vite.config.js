import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// In development (npm run dev) /api is forwarded to the gateway, like nginx does in Docker,
// so the browser only ever talks to one origin.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})

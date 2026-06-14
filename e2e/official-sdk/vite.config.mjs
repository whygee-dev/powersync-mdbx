import { defineConfig } from 'vite';

const serviceUrl = process.env.POWERSYNC_ENDPOINT;

export default defineConfig({
  worker: {
    format: 'es'
  },
  server: serviceUrl
    ? {
        proxy: {
          '/powersync': {
            target: serviceUrl,
            changeOrigin: true,
            rewrite: (path) => path.replace(/^\/powersync/, '')
          }
        }
      }
    : undefined
});

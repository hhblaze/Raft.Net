import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/api': { target: 'http://127.0.0.1:7400', changeOrigin: false } } },
  build: {
    sourcemap: false,
    rolldownOptions: { output: { strictExecutionOrder: true, codeSplitting: { groups: [
      { name: 'react-vendor', test: /node_modules[\\/]react/, priority: 20 },
      { name: 'ui-vendor', test: /node_modules[\\/](antd|@ant-design|@rc-component)/, priority: 15, maxSize: 400000 },
      { name: 'vendor', test: /node_modules/, priority: 10 },
    ] } } },
  },
});

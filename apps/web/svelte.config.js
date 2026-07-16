import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

export default {
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter({ fallback: 'index.html' }),
    csp: {
      mode: 'hash',
      directives: {
        'default-src': ['self'],
        'connect-src': ['self', 'ws:', 'data:'],
        'img-src': ['self', 'data:'],
        'style-src': ['self', 'unsafe-inline'],
        'script-src': ['self', 'wasm-unsafe-eval'],
        'object-src': ['none'],
        'base-uri': ['none']
      }
    }
  }
};

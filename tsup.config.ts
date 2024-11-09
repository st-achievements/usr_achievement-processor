import { tsupConfig } from '@st-api/config';
import { defineConfig } from 'tsup';
import fsp from 'node:fs/promises';

export default defineConfig({
  ...tsupConfig,
  dts: false,
  external: ['vitest'],
  plugins: [
    {
      name: 'delete-dot-env',
      buildEnd: async () => {
        await fsp.rm('.env', {
          force: true,
        });
      },
    },
  ],
});

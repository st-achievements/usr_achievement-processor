import { spawn } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { NodeEnvEnum } from '@st-api/core';

writeFileSync('.env', `API_KEY=\nNODE_ENV=${NodeEnvEnum.Development}`);

spawn('npm', ['run', 'build:watch'], {
  stdio: 'pipe',
  shell: true,
});

spawn('npm', ['run', 'emulators:start'], {
  stdio: 'inherit',
  shell: true,
});

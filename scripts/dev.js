import { spawn } from 'node:child_process';
import { writeFileSync } from 'node:fs';

writeFileSync('.env', `API_KEY=`);

spawn('npm', ['run', 'build:watch'], {
  stdio: 'pipe',
  shell: true,
});

spawn('npm', ['run', 'emulators:start'], {
  stdio: 'inherit',
  shell: true,
});

import { DATABASE_CONNECTION_STRING } from '@st-achievements/database';
import { StFirebaseApp } from '@st-api/firebase';
import dayjs from 'dayjs';
import weekOfYear from 'dayjs/plugin/weekOfYear.js';

import { appHandler } from './app.handler.js';
import { AppModule } from './app.module.js';
import { REDIS_CREDENTIALS } from './redis.provider.js';

dayjs.extend(weekOfYear);

const app = StFirebaseApp.create(AppModule, {
  secrets: [DATABASE_CONNECTION_STRING, REDIS_CREDENTIALS],
}).addPubSub(appHandler);

export const usr_achievement = {
  processor: {
    events: app.getCloudEventHandlers(),
    http: app.getHttpHandler(),
  },
};

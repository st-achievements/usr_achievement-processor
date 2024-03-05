import { DATABASE_CONNECTION_STRING } from '@st-achievements/database';
import { StFirebaseApp } from '@st-api/firebase';

import { appHandler } from './app.handler.js';
import { AppModule } from './app.module.js';

const app = StFirebaseApp.create(AppModule, {
  secrets: [DATABASE_CONNECTION_STRING],
}).addPubSub(appHandler);

export const usr_workout = {
  processor: {
    events: app.getCloudEventHandlers(),
    http: app.getHttpHandler(),
  },
};

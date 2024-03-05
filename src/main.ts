import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ExpressAdapter } from '@nestjs/platform-express';
import { configureApp } from '@st-api/core';
import express from 'express';
import { logger } from 'firebase-functions/v2';

import { AppModule } from './app.module.js';

async function bootstrap() {
  const expressApp = express();
  const app = configureApp(
    await NestFactory.create(AppModule, new ExpressAdapter(expressApp), {
      logger,
    }),
    {
      swagger: {},
    },
  );
  await app.init();
  const isDev = process.env.NODE_ENV !== 'production';
  const host = process.env.HOST ?? 'localhost';
  const port = process.env.PORT ?? 3000;
  const protocol = isDev ? 'http' : 'https';
  await app.listen(port, host);
  Logger.log(`Listening at ${protocol}://${host}:${port}`);
  Logger.log(`Help at ${protocol}://${host}:${port}/help`);
}
bootstrap();

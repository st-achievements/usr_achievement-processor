import { Module } from '@nestjs/common';
import { RedisModule } from '@st-achievements/core';
import { DrizzleOrmModule } from '@st-achievements/database';
import { CoreModule } from '@st-api/core';
import { FirebaseAdminModule } from '@st-api/firebase';

import { AppHandler } from './app.handler.js';
import { LockService } from './lock.service.js';
import { PlatinumService } from './platinum.service.js';

@Module({
  imports: [CoreModule.forRoot(), DrizzleOrmModule, RedisModule, FirebaseAdminModule.forRoot()],
  controllers: [],
  providers: [AppHandler, PlatinumService, LockService],
})
export class AppModule {}

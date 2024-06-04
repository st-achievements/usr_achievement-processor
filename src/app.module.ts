import { Module } from '@nestjs/common';
import { AchievementsCoreModule, RedisModule } from '@st-achievements/core';
import { CoreModule } from '@st-api/core';

import { AppHandler } from './app.handler.js';
import { LockService } from './lock.service.js';
import { PlatinumService } from './platinum.service.js';

@Module({
  imports: [
    CoreModule.forRoot(),
    RedisModule,
    AchievementsCoreModule.forRoot({
      authentication: false,
      throttling: false,
    }),
  ],
  controllers: [],
  providers: [AppHandler, PlatinumService, LockService],
})
export class AppModule {}

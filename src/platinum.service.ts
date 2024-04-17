import { Injectable } from '@nestjs/common';
import { ach, Drizzle, usr } from '@st-achievements/database';
import { getCorrelationId } from '@st-api/core';
import { Eventarc, Logger } from '@st-api/firebase';
import { and, count, eq, ne, sql } from 'drizzle-orm';
import { unionAll } from 'drizzle-orm/pg-core';

import { AchievementCreatedEventDto } from './achievement-created-event.dto.js';
import { AchievementInputDto } from './achievement-input.dto.js';
import { AchievementLevelEnum } from './achievement-level.enum.js';
import {
  ACHIEVEMENT_CREATED_EVENT,
  ACHIEVEMENT_PLATINUM_CREATED_EVENT,
} from './app.constants.js';
import { PLATINUM_NOT_FOUND } from './exceptions.js';

@Injectable()
export class PlatinumService {
  constructor(
    private readonly drizzle: Drizzle,
    private readonly eventarc: Eventarc,
  ) {}

  private readonly logger = Logger.create(this);

  private readonly achievementCountLabel = 'achievement';
  private readonly userAchievementCountLabel = 'user_achievement';

  async checkForPlatinum(data: AchievementInputDto): Promise<void> {
    const [userAchievementPlatinum] = await this.drizzle
      .select({
        achievedAt: usr.achievement.achievedAt,
      })
      .from(usr.achievement)
      .innerJoin(
        ach.achievement,
        eq(usr.achievement.achAchievementId, ach.achievement.id),
      )
      .where(
        and(
          eq(usr.achievement.active, true),
          eq(usr.achievement.userId, data.userId),
          eq(usr.achievement.periodId, data.periodId),
          eq(ach.achievement.levelId, AchievementLevelEnum.Platinum),
        ),
      );

    if (userAchievementPlatinum) {
      this.logger.info(
        `Platinum already achieved for ` +
          `userId = ${data.userId} ` +
          `on periodId = ${data.periodId} ` +
          `at ${userAchievementPlatinum.achievedAt}`,
      );
      return;
    }

    const achievementCountQuery = this.drizzle
      .select({
        type: sql`${this.achievementCountLabel}`,
        count: count(),
      })
      .from(ach.achievement)
      .where(
        and(
          ne(ach.achievement.levelId, AchievementLevelEnum.Platinum),
          eq(ach.achievement.active, true),
        ),
      );
    const userAchievementQuery = this.drizzle
      .select({
        type: sql`${this.userAchievementCountLabel}`,
        count: count(),
      })
      .from(usr.achievement)
      .innerJoin(
        ach.achievement,
        eq(ach.achievement.id, usr.achievement.achAchievementId),
      )
      .where(
        and(
          eq(usr.achievement.active, true),
          eq(usr.achievement.userId, data.userId),
          eq(usr.achievement.periodId, data.periodId),
          ne(ach.achievement.levelId, AchievementLevelEnum.Platinum),
          eq(ach.achievement.active, true),
        ),
      );
    const countResults = await unionAll(
      achievementCountQuery,
      userAchievementQuery,
    );
    const countAchievements =
      countResults.find((result) => result.type === this.achievementCountLabel)
        ?.count ?? 0;
    const countUserAchievements =
      countResults.find(
        (result) => result.type === this.userAchievementCountLabel,
      )?.count ?? 0;
    if (countUserAchievements < countAchievements) {
      this.logger.info(
        `There's still ${countAchievements - countUserAchievements} achievements ` +
          `for userId = ${data.userId} ` +
          `on periodId = ${data.periodId} to earn the platinum`,
      );
      return;
    }

    const platinum = await this.drizzle.query.achAchievement.findFirst({
      where: and(
        eq(ach.achievement.levelId, AchievementLevelEnum.Platinum),
        eq(ach.achievement.active, true),
      ),
      columns: {
        id: true,
        levelId: true,
      },
    });

    if (!platinum) {
      throw PLATINUM_NOT_FOUND();
    }

    await this.drizzle.insert(usr.achievement).values({
      userId: data.userId,
      periodId: data.periodId,
      achievedAt: data.workoutDate,
      achAchievementId: platinum.id,
      metadata: {
        correlationId: getCorrelationId(),
      },
    });

    const platinumEvent: AchievementCreatedEventDto = {
      achievedAt: data.workoutDate.toISOString(),
      periodId: data.periodId,
      userId: data.userId,
      achievementId: platinum.id,
      levelId: platinum.levelId,
    };

    this.logger.info(`platinumEvent`, { platinumEvent });

    await this.eventarc.publish([
      {
        type: ACHIEVEMENT_CREATED_EVENT,
        body: platinumEvent,
      },
      {
        type: ACHIEVEMENT_PLATINUM_CREATED_EVENT,
        body: platinumEvent,
      },
    ]);
  }
}

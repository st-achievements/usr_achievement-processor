import { Injectable } from '@nestjs/common';
import { ach, Drizzle, usr } from '@st-achievements/database';
import {
  createPubSubHandler,
  Eventarc,
  Logger,
  PubSubEventData,
  PubSubHandler,
} from '@st-api/firebase';
import dayjs, { OpUnitType } from 'dayjs';
import { and, count, eq, gte, inArray, lte, sql, SQL, sum } from 'drizzle-orm';

import { AchievementInputDto } from './achievement-input.dto.js';
import {
  ACHIEVEMENT_CREATED_EVENT,
  ACHIEVEMENT_PROCESSOR_QUEUE,
} from './app.constants.js';
import { PlatinumService } from './platinum.service.js';
import { AchievementCreatedEventDto } from './achievement-created-event.dto.js';
import { PgColumn } from 'drizzle-orm/pg-core';

@Injectable()
export class AppHandler implements PubSubHandler<typeof AchievementInputDto> {
  constructor(
    private readonly eventarc: Eventarc,
    private readonly drizzle: Drizzle,
    private readonly platinumService: PlatinumService,
  ) {}

  private readonly logger = Logger.create(this);

  async handle(
    event: PubSubEventData<typeof AchievementInputDto>,
  ): Promise<void> {
    this.logger.log('event', { event });
    const userAchievement = await this.drizzle.query.usrAchievement.findFirst({
      where: and(
        eq(usr.achievement.userId, event.data.userId),
        eq(usr.achievement.periodId, event.data.periodId),
        eq(usr.achievement.achAchievementId, event.data.achievementId),
        eq(usr.achievement.active, true),
      ),
      columns: {
        id: true,
      },
    });
    if (userAchievement) {
      this.logger.log(
        `achievementId = ${event.data.achievementId} ` +
          `already acquired for userId = ${event.data.userId} ` +
          `on periodId = ${event.data.periodId}`,
      );
      return;
    }
    const achievement = await this.drizzle.query.achAchievement.findFirst({
      where: and(
        eq(ach.achievement.active, true),
        eq(ach.achievement.id, event.data.achievementId),
      ),
      with: {
        cfgQuantityUnit: true,
        achievementWorkoutTypes: true,
      },
    });
    if (!achievement) {
      this.logger.warn(
        `achievementId = ${event.data.achievementId} does not exists or is inactive`,
      );
      return;
    }

    const whereWorkouts: SQL[] = [
      eq(usr.workout.userId, event.data.userId),
      eq(usr.workout.active, true),
      eq(usr.workout.periodId, event.data.periodId),
    ];

    const fromConditionToDayjsUnit = new Map<
      ach.PeriodConditionType,
      OpUnitType
    >()
      .set('sameDay', 'day')
      .set('sameWeek', 'week')
      .set('sameMonth', 'month');

    switch (achievement.periodCondition) {
      case 'sameDay':
      case 'sameWeek':
      case 'sameMonth': {
        const unit = fromConditionToDayjsUnit.get(achievement.periodCondition);
        if (!unit) {
          break;
        }
        const startOf = dayjs(event.data.workoutDate).startOf(unit).toDate();
        const endOf = dayjs(event.data.workoutDate).endOf(unit).toDate();
        whereWorkouts.push(
          gte(usr.workout.startedAt, startOf),
          lte(usr.workout.endedAt, endOf),
        );
        break;
      }
      case 'singleSession': {
        whereWorkouts.push(eq(usr.workout.id, event.data.workoutId));
        break;
      }
    }

    const select: { value: SQL<number>; by?: PgColumn } = {
      value: sql`0`,
    };
    let checkForCompleteness: (
      values: { value: number; by?: unknown }[],
    ) => boolean = ([value]: { value: number }[]) =>
      (value?.value ?? Number.NEGATIVE_INFINITY) >= achievement.quantityNeeded;

    switch (achievement.workoutTypeCondition) {
      case 'exclusiveAnyOf':
      case 'anyOf': {
        whereWorkouts.push(
          inArray(
            usr.workout.workoutTypeId,
            achievement.achievementWorkoutTypes.map(
              (workoutType) => workoutType.workoutTypeId,
            ),
          ),
        );
        if (achievement.workoutTypeCondition === 'exclusiveAnyOf') {
          select.by = usr.workout.workoutTypeId;
          checkForCompleteness = (values) =>
            values.length >= achievement.quantityNeeded;
        }
        break;
      }
      case 'exclusiveAny': {
        select.by = usr.workout.workoutTypeId;
        checkForCompleteness = (values) =>
          values.length >= achievement.quantityNeeded;
        break;
      }
    }

    switch (achievement.quantityUnitId) {
      case 1: {
        select.value = sql`sum(${usr.workout.distance}) * 1000`.mapWith(Number);
        break;
      }
      case 2: {
        select.value = sum(usr.workout.distance).mapWith(Number);
        break;
      }
      case 3: {
        select.value = sum(usr.workout.energyBurned).mapWith(Number);
        break;
      }
      case 4: {
        select.value = count();
        break;
      }
      case 6: {
        select.value = sum(usr.workout.duration).mapWith(Number);
        break;
      }
      case 7: {
        select.value = sql`sum(${usr.workout.duration}) / 60`.mapWith(Number);
        break;
      }
    }

    const query = this.drizzle
      .select(select)
      .from(usr.workout)
      .where(and(...whereWorkouts));

    if (select.by) {
      query.groupBy(select.by);
    }

    const values = await query.execute();

    if (checkForCompleteness(values)) {
      await this.drizzle.insert(usr.achievement).values({
        userId: event.data.userId,
        achAchievementId: event.data.achievementId,
        achievedAt: event.data.workoutDate,
        periodId: event.data.periodId,
      });
      const achievementCreatedEvent: AchievementCreatedEventDto = {
        achievedAt: event.data.workoutDate.toISOString(),
        periodId: event.data.periodId,
        userId: event.data.userId,
        achievementId: event.data.achievementId,
        levelId: achievement.levelId,
      };
      await this.eventarc.publish({
        type: ACHIEVEMENT_CREATED_EVENT,
        body: achievementCreatedEvent,
      });
    }

    // await this.platinumService.checkForPlatinum(event.data);
  }
}

export const appHandler = createPubSubHandler({
  handler: AppHandler,
  schema: () => AchievementInputDto,
  topic: ACHIEVEMENT_PROCESSOR_QUEUE,
});

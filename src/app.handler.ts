import { Injectable } from '@nestjs/common';
import { ach, cfg, Drizzle, usr } from '@st-achievements/database';
import { safeAsync } from '@st-api/core';
import {
  createPubSubHandler,
  Eventarc,
  Logger,
  PubSubEventData,
  PubSubHandler,
} from '@st-api/firebase';
import { and, eq, inArray } from 'drizzle-orm';

import { AchievementCreatedEventDto } from './achievement-created-event.dto.js';
import { AchievementInputDto } from './achievement-input.dto.js';
import { AchievementProgressDto } from './achievement-progress.dto.js';
import {
  ACHIEVEMENT_CREATED_EVENT,
  ACHIEVEMENT_PROCESSOR_QUEUE,
  ACHIEVEMENT_PROGRESS_CREATED_EVENT,
} from './app.constants.js';
import { PERIOD_NOT_FOUND } from './exceptions.js';
import { LockService } from './lock.service.js';
import { PlatinumService } from './platinum.service.js';
import { QueryProcessor } from './query/query.js';
import { FrequencyEveryDayInSameMonthOperator } from './query-operator/frequency-every-day-in-same-month.operator.js';
import { FrequencyEveryDayInSamePeriodOperator } from './query-operator/frequency-every-day-in-same-period.operator.js';
import { FrequencyEveryDayInSameWeekOperator } from './query-operator/frequency-every-day-in-same-week.operator.js';
import { FrequencyEveryMonthInSamePeriodOperator } from './query-operator/frequency-every-month-in-same-period.operator.js';
import { FrequencyEveryWeekInSameMonthOperator } from './query-operator/frequency-every-week-in-same-month.operator.js';
import { FrequencyEveryWeekInSamePeriodOperator } from './query-operator/frequency-every-week-in-same-period.operator.js';
import { InitialOperator } from './query-operator/initial.operator.js';
import { PeriodConditionSameOperator } from './query-operator/period-condition-same.operator.js';
import { PeriodConditionSingleOperator } from './query-operator/period-condition-single.operator.js';
import { QuantityUnitCaloriesOperator } from './query-operator/quantity-unit-calories.operator.js';
import { QuantityUnitExerciseOperator } from './query-operator/quantity-unit-exercise.operator.js';
import { QuantityUnitHourOperator } from './query-operator/quantity-unit-hour.operator.js';
import { QuantityUnitKMOperator } from './query-operator/quantity-unit-km.operator.js';
import { QuantityUnitMeterOperator } from './query-operator/quantity-unit-meter.operator.js';
import { QuantityUnitMinuteOperator } from './query-operator/quantity-unit-minute.operator.js';
import { WorkoutTypeConditionAllOfOperator } from './query-operator/workout-type-condition-all-of.operator.js';
import { WorkoutTypeConditionAnyOfOperator } from './query-operator/workout-type-condition-any-of.operator.js';
import { WorkoutTypeConditionExclusiveAnyOfOperator } from './query-operator/workout-type-condition-exclusive-any-of.operator.js';
import { WorkoutTypeConditionExclusiveAnyOperator } from './query-operator/workout-type-condition-exclusive-any.operator.js';

@Injectable()
export class AppHandler implements PubSubHandler<typeof AchievementInputDto> {
  constructor(
    private readonly eventarc: Eventarc,
    private readonly drizzle: Drizzle,
    private readonly platinumService: PlatinumService,
    private readonly lockService: LockService,
  ) {}

  private readonly logger = Logger.create(this);

  async handle(
    event: PubSubEventData<typeof AchievementInputDto>,
  ): Promise<void> {
    this.logger.info({ event });
    const lockKey = `user_id=${event.data.userId}`;
    this.logger.info({ lockKey });
    await this.lockService.assert(lockKey);

    const [error] = await safeAsync(() => this.execute(event));

    await this.lockService.release(lockKey);

    if (error) {
      throw error;
    }
  }

  private async execute(event: PubSubEventData<typeof AchievementInputDto>) {
    const period = await this.drizzle.query.cfgPeriod.findFirst({
      where: and(
        eq(cfg.period.id, event.data.periodId),
        eq(cfg.period.active, true),
      ),
    });

    this.logger.info({ period });

    if (!period) {
      throw PERIOD_NOT_FOUND();
    }

    const achievements = await this.drizzle.query.achAchievement.findMany({
      where: and(
        eq(ach.achievement.active, true),
        inArray(ach.achievement.id, event.data.achievementIds),
      ),
      with: {
        cfgQuantityUnit: true,
        achievementWorkoutTypes: true,
      },
    });

    this.logger.info({ achievements });

    const userAchievements = await this.drizzle.query.usrAchievement.findMany({
      where: and(
        eq(usr.achievement.userId, event.data.userId),
        eq(usr.achievement.periodId, event.data.periodId),
        inArray(usr.achievement.achAchievementId, event.data.achievementIds),
        eq(usr.achievement.active, true),
      ),
      columns: {
        id: true,
      },
    });

    this.logger.info({ userAchievements });

    const eventsToPublish: Promise<unknown>[] = [];

    for (const achievementId of event.data.achievementIds) {
      this.logger.log(`started processing achievementId = ${achievementId}`, {
        event,
      });
      const hasUserAchievement = userAchievements.some(
        (userAchievement) => userAchievement.id === achievementId,
      );
      if (hasUserAchievement) {
        this.logger.log(
          `achievementId = ${achievementId} ` +
            `already acquired for userId = ${event.data.userId} ` +
            `on periodId = ${event.data.periodId}`,
        );
        continue;
      }

      const achievement = achievements.find(({ id }) => id === achievementId);

      if (!achievement) {
        this.logger.warn(
          `achievementId = ${achievementId} does not exists or is inactive`,
        );
        continue;
      }

      const queryFilter = new QueryProcessor()
        .pipe(
          InitialOperator,
          PeriodConditionSameOperator,
          PeriodConditionSingleOperator,
          QuantityUnitMeterOperator,
          QuantityUnitKMOperator,
          QuantityUnitCaloriesOperator,
          QuantityUnitExerciseOperator,
          QuantityUnitMinuteOperator,
          QuantityUnitHourOperator,
          WorkoutTypeConditionExclusiveAnyOfOperator,
          WorkoutTypeConditionAnyOfOperator,
          WorkoutTypeConditionExclusiveAnyOperator,
          WorkoutTypeConditionAllOfOperator,
          FrequencyEveryDayInSameMonthOperator,
          FrequencyEveryWeekInSameMonthOperator,
          FrequencyEveryDayInSamePeriodOperator,
          FrequencyEveryMonthInSamePeriodOperator,
          FrequencyEveryWeekInSamePeriodOperator,
          FrequencyEveryDayInSameWeekOperator,
        )
        .execute({
          input: event.data,
          achievement,
          achievementWorkoutTypes: achievement.achievementWorkoutTypes,
          period,
        })
        .get();

      const query = this.drizzle
        .select(queryFilter.select)
        .from(usr.workout)
        .where(and(...queryFilter.where));

      if (queryFilter.select.by) {
        query.groupBy(queryFilter.select.by);
      }

      const values = await query.execute();

      if (queryFilter.isComplete(values)) {
        const [userAchievementCreated] = await this.drizzle
          .insert(usr.achievement)
          .values({
            userId: event.data.userId,
            achAchievementId: achievementId,
            achievedAt: event.data.workoutDate,
            periodId: event.data.periodId,
          })
          .returning({
            userAchievementId: usr.achievement.id,
          });
        const achievementCreatedEvent: AchievementCreatedEventDto = {
          achievedAt: event.data.workoutDate.toISOString(),
          periodId: event.data.periodId,
          userId: event.data.userId,
          achievementId,
          levelId: achievement.levelId,
          userAchievementId: userAchievementCreated!.userAchievementId,
        };
        eventsToPublish.push(
          this.eventarc.publish({
            type: ACHIEVEMENT_CREATED_EVENT,
            body: achievementCreatedEvent,
          }),
        );
      } else if (achievement.hasProgressTracking) {
        const quantity = Math.floor(queryFilter.getProgressQuantity(values));
        await this.drizzle
          .insert(usr.achievementProgress)
          .values({
            userId: event.data.userId,
            achAchievementId: achievementId,
            periodId: event.data.periodId,
            quantity,
          })
          .onConflictDoUpdate({
            target: [
              usr.achievementProgress.achAchievementId,
              usr.achievementProgress.userId,
              usr.achievementProgress.periodId,
            ],
            set: {
              quantity,
            },
          });
        const achievementProgressCreatedDto: AchievementProgressDto = {
          achievementId,
          periodId: event.data.periodId,
          userId: event.data.userId,
          quantity,
        };
        eventsToPublish.push(
          this.eventarc.publish({
            type: ACHIEVEMENT_PROGRESS_CREATED_EVENT,
            body: achievementProgressCreatedDto,
          }),
        );
      }
    }

    await Promise.all(eventsToPublish);
    await this.platinumService.checkForPlatinum(event.data);
  }
}

export const appHandler = createPubSubHandler({
  handler: AppHandler,
  schema: () => AchievementInputDto,
  topic: ACHIEVEMENT_PROCESSOR_QUEUE,
  retry: true,
  preserveExternalChanges: true,
});

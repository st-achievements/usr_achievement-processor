import { Injectable } from '@nestjs/common';
import { ach, cfg, Drizzle, usr } from '@st-achievements/database';
import {
  createPubSubHandler,
  Eventarc,
  Logger,
  PubSubEventData,
  PubSubHandler,
} from '@st-api/firebase';
import { and, eq } from 'drizzle-orm';

import { AchievementCreatedEventDto } from './achievement-created-event.dto.js';
import { AchievementInputDto } from './achievement-input.dto.js';
import { AchievementProgressDto } from './achievement-progress.dto.js';
import {
  ACHIEVEMENT_CREATED_EVENT,
  ACHIEVEMENT_PROCESSOR_QUEUE,
  ACHIEVEMENT_PROGRESS_CREATED_EVENT,
  ACHIEVEMENT_PROGRESS_UPDATED_EVENT,
} from './app.constants.js';
import { PERIOD_NOT_FOUND } from './exceptions.js';
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

    const period = await this.drizzle.query.cfgPeriod.findFirst({
      where: and(
        eq(cfg.period.id, event.data.periodId),
        eq(cfg.period.active, true),
      ),
    });

    if (!period) {
      throw PERIOD_NOT_FOUND();
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
          achAchievementId: event.data.achievementId,
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
        achievementId: event.data.achievementId,
        levelId: achievement.levelId,
        userAchievementId: userAchievementCreated!.userAchievementId,
      };
      await this.eventarc.publish({
        type: ACHIEVEMENT_CREATED_EVENT,
        body: achievementCreatedEvent,
      });
    } else if (achievement.hasProgressTracking) {
      const quantity = Math.floor(queryFilter.getProgressQuantity(values));
      const achievementProgress =
        await this.drizzle.query.usrAchievementProgress.findFirst({
          where: and(
            eq(
              usr.achievementProgress.achAchievementId,
              event.data.achievementId,
            ),
            eq(usr.achievementProgress.userId, event.data.userId),
            eq(usr.achievementProgress.periodId, event.data.periodId),
            eq(usr.achievementProgress.active, true),
          ),
          columns: {
            id: true,
            quantity: true,
          },
        });
      let userAchievementProgressId: number;
      let eventType: string;
      if (achievementProgress) {
        userAchievementProgressId = achievementProgress.id;
        eventType = ACHIEVEMENT_PROGRESS_UPDATED_EVENT;
        if (achievementProgress.quantity !== quantity) {
          await this.drizzle
            .update(usr.achievementProgress)
            .set({
              quantity,
            })
            .where(eq(usr.achievementProgress.id, achievementProgress.id));
        }
      } else {
        const [achievementProgressCreated] = await this.drizzle
          .insert(usr.achievementProgress)
          .values({
            userId: event.data.userId,
            achAchievementId: event.data.achievementId,
            periodId: event.data.periodId,
            quantity,
          })
          .returning({
            id: usr.achievementProgress.id,
          });
        userAchievementProgressId = achievementProgressCreated!.id;
        eventType = ACHIEVEMENT_PROGRESS_CREATED_EVENT;
      }
      const achievementProgressCreatedDto: AchievementProgressDto = {
        userAchievementProgressId,
        achievementId: event.data.achievementId,
        periodId: event.data.periodId,
        userId: event.data.userId,
        quantity,
      };
      await this.eventarc.publish({
        type: eventType,
        body: achievementProgressCreatedDto,
      });
    }

    await this.platinumService.checkForPlatinum(event.data);
  }
}

export const appHandler = createPubSubHandler({
  handler: AppHandler,
  schema: () => AchievementInputDto,
  topic: ACHIEVEMENT_PROCESSOR_QUEUE,
});

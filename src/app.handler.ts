import { Injectable } from '@nestjs/common';
import { ach, cfg, Drizzle, usr } from '@st-achievements/database';
import { getCorrelationId, safeAsync } from '@st-api/core';
import {
  createPubSubHandler,
  Eventarc,
  Logger,
  PubSubEventData,
  PubSubHandler,
} from '@st-api/firebase';
import {
  and,
  eq,
  inArray,
  InferInsertModel,
  InferSelectModel,
  sql,
} from 'drizzle-orm';

import { AchievementCreatedEventDto } from './achievement-created-event.dto.js';
import { AchievementInputDto } from './achievement-input.dto.js';
import { AchievementProgressDto } from './achievement-progress.dto.js';
import {
  ACHIEVEMENT_CREATED_EVENT,
  ACHIEVEMENT_PROCESSOR_QUEUE,
  ACHIEVEMENT_PROGRESS_CREATED_EVENT,
} from './app.constants.js';
import { PERIOD_NOT_FOUND, WORKOUT_NOT_FOUND } from './exceptions.js';
import { LockService } from './lock.service.js';
import { PlatinumService } from './platinum.service.js';
import { QueryProcessor } from './query/query.js';
import { QueryProcessed } from './query/query.type.js';
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

interface QueryBuilder {
  achievement: InferSelectModel<typeof ach.achievement>;
  queryFilter: QueryProcessed;
}

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
    Logger.setContext(`u${event.data.userId}-w${event.data.workoutId}`);
    this.logger.info(`started`, { event });
    const lockKey = this.lockService.createKey(String(event.data.userId));
    this.logger.info(`lockKey = ${lockKey}`);
    await this.lockService.assert(lockKey);

    const [error] = await safeAsync(() => this.execute(event));

    await this.lockService.release(lockKey);

    if (error) {
      this.logger.error(`finished with error`, { error });
      throw error;
    }

    this.logger.info('finished successfully');
  }

  private async execute(event: PubSubEventData<typeof AchievementInputDto>) {
    const workout = await this.drizzle.query.usrWorkout.findFirst({
      where: and(eq(usr.workout.id, event.data.workoutId)),
      columns: {
        achievementProcessedAt: true,
        metadata: true,
      },
    });

    if (!workout) {
      throw WORKOUT_NOT_FOUND();
    }

    if (workout.achievementProcessedAt) {
      this.logger.info(
        `workout_id = ${event.data.workoutId} already processed at ${workout.achievementProcessedAt.toISOString()}`,
      );
      return;
    }

    const period = await this.drizzle.query.cfgPeriod.findFirst({
      where: and(
        eq(cfg.period.id, event.data.periodId),
        eq(cfg.period.active, true),
      ),
    });

    this.logger.info('period', { period });

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

    this.logger.info('achievements', { achievements });

    const userAchievements = await this.drizzle.query.usrAchievement.findMany({
      where: and(
        eq(usr.achievement.userId, event.data.userId),
        eq(usr.achievement.periodId, event.data.periodId),
        inArray(usr.achievement.achAchievementId, event.data.achievementIds),
        eq(usr.achievement.active, true),
      ),
      columns: {
        userId: true,
        periodId: true,
        achAchievementId: true,
      },
    });

    this.logger.info('userAchievements', { userAchievements });

    const queries: QueryBuilder[] = [];

    for (const achievementId of event.data.achievementIds) {
      this.logger.log(`started checking achievementId = ${achievementId}`);
      const hasUserAchievement = userAchievements.some(
        (userAchievement) => userAchievement.achAchievementId === achievementId,
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

      queries.push({
        achievement,
        queryFilter,
      });

      this.logger.info(
        `added achievementId = ${achievement.id} to the list of processing`,
      );
    }

    const [firstQueryBuilder, ...restQueryBuilders] = queries;

    if (!firstQueryBuilder) {
      this.logger.warn('Could not build the final query');
      return;
    }

    const finalQuery = this.buildQuery(firstQueryBuilder);

    for (const queryBuilder of restQueryBuilders) {
      finalQuery.unionAll(this.buildQuery(queryBuilder));
    }

    this.logger.info('final query built');

    const finalResult = await finalQuery.execute();

    this.logger.debug('finalResult', { finalResult });

    const insertAchievement: InferInsertModel<typeof usr.achievement>[] = [];
    const insertProgress: InferInsertModel<typeof usr.achievementProgress>[] =
      [];
    const eventsToPublish: Array<
      | {
          type: typeof ACHIEVEMENT_CREATED_EVENT;
          body: AchievementCreatedEventDto;
        }
      | {
          type: typeof ACHIEVEMENT_PROGRESS_CREATED_EVENT;
          body: AchievementProgressDto;
        }
    > = [];

    const correlationId = getCorrelationId();

    for (const { queryFilter, achievement } of queries) {
      const values = finalResult.filter(
        (result) => result.achievementId === achievement.id,
      );
      const quantity = Math.floor(queryFilter.getProgressQuantity(values));
      if (queryFilter.isComplete(values)) {
        insertAchievement.push({
          achAchievementId: achievement.id,
          userId: event.data.userId,
          achievedAt: event.data.workoutDate,
          periodId: event.data.periodId,
          metadata: {
            correlationId,
          },
        });
        eventsToPublish.push({
          type: ACHIEVEMENT_CREATED_EVENT,
          body: {
            achievementId: achievement.id,
            userId: event.data.userId,
            achievedAt: event.data.workoutDate.toISOString(),
            periodId: event.data.periodId,
            levelId: achievement.levelId,
          },
        });
      } else if (achievement.hasProgressTracking && quantity > 0) {
        insertProgress.push({
          userId: event.data.userId,
          achAchievementId: achievement.id,
          periodId: event.data.periodId,
          quantity,
          metadata: {
            correlationId,
          },
        });
        eventsToPublish.push({
          type: ACHIEVEMENT_PROGRESS_CREATED_EVENT,
          body: {
            achievementId: achievement.id,
            userId: event.data.userId,
            periodId: event.data.periodId,
            quantity,
          },
        });
      }
    }

    this.logger.debug('insertions and events', {
      insertAchievement,
      insertProgress,
      eventsToPublish,
    });

    if (!insertAchievement.length && !insertProgress.length) {
      this.logger.info('no achievement nor progress was made');
      return;
    }

    await this.drizzle.transaction(async (transaction) => {
      if (insertAchievement.length) {
        await transaction
          .insert(usr.achievement)
          .values(insertAchievement)
          .onConflictDoNothing();
      }
      if (insertProgress) {
        await transaction
          .insert(usr.achievementProgress)
          .values(insertProgress)
          .onConflictDoUpdate({
            target: [
              usr.achievementProgress.achAchievementId,
              usr.achievementProgress.userId,
              usr.achievementProgress.periodId,
            ],
            set: {
              quantity: sql`excluded.quantity`,
            },
          });
      }
      await transaction
        .update(usr.workout)
        .set({
          achievementProcessedAt: new Date(),
          metadata: {
            ...workout.metadata,
            achievementProcessedCorrelationId: getCorrelationId(),
          },
        })
        .where(eq(usr.workout.id, event.data.workoutId));
    });

    this.logger.info('All inserted/updated in the database successfully!');

    await this.eventarc.publish(eventsToPublish);

    this.logger.info('Published all events!');

    await this.platinumService.checkForPlatinum(event.data);
  }

  private buildQuery({ queryFilter, achievement }: QueryBuilder) {
    const query = this.drizzle
      .select({
        value: queryFilter.select.value,
        by: queryFilter.select.by ?? sql`0`,
        achievementId: sql`${achievement.id}`.mapWith(Number),
      })
      .from(usr.workout)
      .where(and(...queryFilter.where));
    if (queryFilter.select.by) {
      query.groupBy(queryFilter.select.by);
    }
    return query;
  }
}

export const appHandler = createPubSubHandler({
  handler: AppHandler,
  schema: () => AchievementInputDto,
  topic: ACHIEVEMENT_PROCESSOR_QUEUE,
  retry: true,
  preserveExternalChanges: true,
});

import { Injectable } from '@nestjs/common';
import { ach, cfg, Drizzle, usr } from '@st-achievements/database';
import {
  createPubSubHandler,
  Eventarc,
  Logger,
  PubSubEventData,
  PubSubHandler,
} from '@st-api/firebase';
import dayjs, { OpUnitType } from 'dayjs';
import { and, count, eq, gte, inArray, lte, sql, SQL, sum } from 'drizzle-orm';
import { PgColumn } from 'drizzle-orm/pg-core';

import { AchievementCreatedEventDto } from './achievement-created-event.dto.js';
import { AchievementInputDto } from './achievement-input.dto.js';
import {
  ACHIEVEMENT_CREATED_EVENT,
  ACHIEVEMENT_PROCESSOR_QUEUE,
} from './app.constants.js';
import { PlatinumService } from './platinum.service.js';
import { QuantityUnitEnum } from './quantity-unit.enum.js';

@Injectable()
export class AppHandler implements PubSubHandler<typeof AchievementInputDto> {
  constructor(
    private readonly eventarc: Eventarc,
    private readonly drizzle: Drizzle,
    private readonly platinumService: PlatinumService,
  ) {}

  private readonly logger = Logger.create(this);

  private readonly fromQuantityUnitToSelectValueSQL = new Map<number, SQL>()
    .set(QuantityUnitEnum.Meter, sql`sum(${usr.workout.distance}) * 1000`)
    .set(QuantityUnitEnum.KM, sum(usr.workout.distance))
    .set(QuantityUnitEnum.Calories, sum(usr.workout.energyBurned))
    .set(QuantityUnitEnum.Exercise, count())
    .set(QuantityUnitEnum.Minute, sum(usr.workout.duration))
    .set(QuantityUnitEnum.Hour, sql`sum(${usr.workout.duration}) / 60`);

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
      // TODO
      throw new Error();
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

    const selectValue =
      this.fromQuantityUnitToSelectValueSQL.get(achievement.quantityUnitId) ??
      sql`0`;
    const select: { value: SQL<number>; by?: PgColumn | SQL } = {
      value: selectValue.mapWith(Number),
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
      case 'allOf': {
        select.by = usr.workout.workoutTypeId;
        checkForCompleteness = (values) =>
          values.length >= achievement.achievementWorkoutTypes.length;
        break;
      }
    }

    const fromFrequencyToExtract = new Map<ach.FrequencyType | null, string>()
      .set('day', 'day')
      .set('week', 'day')
      .set('month', 'month');

    const extractSQL = fromFrequencyToExtract.get(achievement.frequency);
    if (extractSQL) {
      select.by = sql`extract(${sql.raw(extractSQL)} from ${usr.workout.startedAt})`;
    }

    if (achievement.frequencyCondition === 'every') {
      switch (achievement.periodCondition) {
        case 'sameMonth': {
          switch (achievement.frequency) {
            case 'day': {
              const endOfMonth = dayjs(event.data.workoutDate).endOf('month');
              const allDaysOfMonth = Array.from(
                { length: endOfMonth.get('date') },
                (_, index) => index + 1,
              );

              checkForCompleteness = (values) => {
                const daysCompleted = new Set(
                  values.map((value) => Number(value.by)),
                );
                this.logger.info('every - sameMonth - day', {
                  endOfMonth: endOfMonth.toDate(),
                  allDaysOfMonth: [...allDaysOfMonth],
                  daysCompleted: [...daysCompleted],
                });
                return allDaysOfMonth.every((day) => daysCompleted.has(day));
              };
              break;
            }
            case 'week': {
              const weeks = new Set<number>();
              const startOfMonth = dayjs(event.data.workoutDate).startOf(
                'month',
              );
              const month = startOfMonth.get('month');
              let date = startOfMonth;
              while (date.get('month') === month) {
                const week = date.week();
                weeks.add(week);
                date = date.add(1, 'day');
              }
              checkForCompleteness = (values) => {
                const daysCompleted = values.map((value) =>
                  dayjs(event.data.workoutDate).set('day', Number(value.by)),
                );
                this.logger.info('every - sameMonth - day', {
                  endOfMonth: startOfMonth.toDate(),
                  allDaysOfMonth: [...weeks],
                  daysCompleted: [...daysCompleted],
                });
                return [...weeks].every((week) =>
                  daysCompleted.some((day) => day.week() === week),
                );
              };
            }
          }
          break;
        }
        case 'sameWeek': {
          if (achievement.frequency !== 'day') {
            break;
          }
          const startOfWeek = dayjs(event.data.workoutDate).startOf('week');
          const daysOfWeek = Array.from({ length: 7 }, (_, index) =>
            startOfWeek.add(index, 'day').get('date'),
          );
          checkForCompleteness = (values) => {
            const daysCompleted = new Set(
              values.map((value) => Number(value.by)),
            );
            this.logger.info('every - sameMonth - day', {
              startOfWeek: startOfWeek.toDate(),
              daysOfWeek: [...daysOfWeek],
              daysCompleted: [...daysCompleted],
            });
            return daysOfWeek.every((day) => daysCompleted.has(day));
          };
          break;
        }
        case 'samePeriod': {
          const endOfPeriod = dayjs(period.endAt);
          switch (achievement.frequency) {
            case 'day': {
              select.by = sql`${usr.workout.startedAt}::date`.mapWith(String);
              const allDaysOfPeriod = new Set<string>();
              let date = dayjs(period.startAt);
              while (date.isBefore(endOfPeriod) || date.isSame(endOfPeriod)) {
                allDaysOfPeriod.add(date.format('YYYY-MM-DD'));
                date = date.add(1, 'day');
              }
              checkForCompleteness = (values) =>
                values.every(({ by }) => allDaysOfPeriod.has(String(by)));
              break;
            }
            case 'week': {
              select.by = sql`extract(year from ${usr.workout.startedAt}) || '-' || extract(week from ${usr.workout.startedAt})`;
              const allWeeksOfPeriod = new Set<string>();
              let date = dayjs(period.startAt);
              while (date.isBefore(endOfPeriod) || date.isSame(endOfPeriod)) {
                allWeeksOfPeriod.add(`${date.get('year')}-${date.week()}`);
                date = date.add(1, 'day');
              }
              checkForCompleteness = (values) =>
                values.every(({ by }) => allWeeksOfPeriod.has(String(by)));
              break;
            }
            case 'month': {
              select.by = sql`to_char(${usr.workout.startedAt}, 'YYYY-MM')`;
              const allMonthsOfPeriod = new Set<string>();
              let date = dayjs(period.startAt);
              while (date.isBefore(endOfPeriod) || date.isSame(endOfPeriod)) {
                allMonthsOfPeriod.add(date.format('YYYY-MM'));
                date = date.add(1, 'day');
              }
              checkForCompleteness = (values) =>
                values.every(({ by }) => allMonthsOfPeriod.has(String(by)));
              break;
            }
          }
          break;
        }
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

    // TODO add achievement progress if available

    await this.platinumService.checkForPlatinum(event.data);
  }
}

export const appHandler = createPubSubHandler({
  handler: AppHandler,
  schema: () => AchievementInputDto,
  topic: ACHIEVEMENT_PROCESSOR_QUEUE,
});

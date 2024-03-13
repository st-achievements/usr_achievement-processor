import { ach, usr } from '@st-achievements/database';
import dayjs, { OpUnitType } from 'dayjs';
import { gte, lte } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query } from '../query/query.type.js';

export class PeriodConditionSameOperator extends QueryOperator {
  private readonly conditions: readonly ach.PeriodConditionType[] = [
    'sameDay',
    'sameWeek',
    'sameMonth',
  ];

  private readonly fromConditionToDayjsUnit = new Map<
    ach.PeriodConditionType,
    OpUnitType
  >()
    .set('sameDay', 'day')
    .set('sameWeek', 'week')
    .set('sameMonth', 'month');

  condition(): boolean {
    return this.conditions.includes(this.options.achievement.periodCondition);
  }

  execute(query: Query): Query {
    const unit = this.fromConditionToDayjsUnit.get(
      this.options.achievement.periodCondition,
    );
    if (!unit) {
      return query;
    }
    const startOf = dayjs(this.options.input.workoutDate)
      .startOf(unit)
      .toDate();
    const endOf = dayjs(this.options.input.workoutDate).endOf(unit).toDate();
    query.where.push(
      gte(usr.workout.startedAt, startOf),
      lte(usr.workout.endedAt, endOf),
    );
    return query;
  }
}

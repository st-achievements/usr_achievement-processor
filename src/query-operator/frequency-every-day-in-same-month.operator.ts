import { usr } from '@st-achievements/database';
import dayjs from 'dayjs';
import { sql } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class FrequencyEveryDayInSameMonthOperator extends QueryOperator {
  condition(): boolean {
    const { frequencyCondition, frequency, periodCondition } =
      this.options.achievement;
    return (
      frequencyCondition === 'every' &&
      frequency === 'day' &&
      periodCondition === 'sameMonth'
    );
  }

  execute(query: Query): Query {
    query.select.by = sql`extract(day from ${usr.workout.startedAt})`;
    return query;
  }

  override isComplete(values: QueryResult[]): boolean {
    const endOfMonth = dayjs(this.options.input.workoutDate).endOf('month');
    const allDaysOfMonth = Array.from(
      { length: endOfMonth.get('date') },
      (_, index) => index + 1,
    );
    const daysCompleted = new Set(values.map((value) => Number(value.by)));
    return allDaysOfMonth.every((day) => daysCompleted.has(day));
  }
}

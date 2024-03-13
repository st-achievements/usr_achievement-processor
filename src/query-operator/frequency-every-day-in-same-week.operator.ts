import { usr } from '@st-achievements/database';
import dayjs from 'dayjs';
import { sql } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class FrequencyEveryDayInSameWeekOperator extends QueryOperator {
  condition(): boolean {
    const { frequencyCondition, frequency, periodCondition } =
      this.options.achievement;
    return (
      frequencyCondition === 'every' &&
      frequency === 'day' &&
      periodCondition === 'sameWeek'
    );
  }

  execute(query: Query): Query {
    query.select.by = sql`extract(day from ${usr.workout.startedAt})`;
    return query;
  }

  override isComplete(values: QueryResult[]): boolean {
    const startOfWeek = dayjs(this.options.input.workoutDate).startOf('week');
    const daysOfWeek = Array.from({ length: 7 }, (_, index) =>
      startOfWeek.add(index, 'day').get('date'),
    );
    const daysCompleted = new Set(values.map((value) => Number(value.by)));
    return daysOfWeek.every((day) => daysCompleted.has(day));
  }
}

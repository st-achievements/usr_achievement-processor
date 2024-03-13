import { usr } from '@st-achievements/database';
import dayjs from 'dayjs';
import { sql } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class FrequencyEveryWeekInSameMonthOperator extends QueryOperator {
  condition(): boolean {
    const { frequencyCondition, frequency, periodCondition } =
      this.options.achievement;
    return (
      frequencyCondition === 'every' &&
      frequency === 'week' &&
      periodCondition === 'sameMonth'
    );
  }

  execute(query: Query): Query {
    query.select.by = sql`extract(day from ${usr.workout.startedAt})`;
    return query;
  }

  override isComplete(values: QueryResult[]): boolean {
    const weeks = new Set<number>();
    const startOfMonth = dayjs(this.options.input.workoutDate).startOf('month');
    const month = startOfMonth.get('month');
    let date = startOfMonth;
    while (date.get('month') === month) {
      const week = date.week();
      weeks.add(week);
      date = date.add(1, 'day');
    }
    const daysCompleted = values.map((value) =>
      dayjs(this.options.input.workoutDate).set('day', Number(value.by)),
    );
    return [...weeks].every((week) =>
      daysCompleted.some((day) => day.week() === week),
    );
  }
}

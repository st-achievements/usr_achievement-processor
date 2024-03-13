import { usr } from '@st-achievements/database';
import dayjs from 'dayjs';
import { sql } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class FrequencyEveryWeekInSamePeriodOperator extends QueryOperator {
  condition(): boolean {
    const { frequencyCondition, frequency, periodCondition } =
      this.options.achievement;
    return (
      frequencyCondition === 'every' &&
      frequency === 'week' &&
      periodCondition === 'samePeriod'
    );
  }

  execute(query: Query): Query {
    query.select.by = sql`extract(year from ${usr.workout.startedAt}) || '-' || extract(week from ${usr.workout.startedAt})`;
    return query;
  }

  override isComplete(values: QueryResult[]): boolean {
    const endOfPeriod = dayjs(this.options.period.endAt);
    const allWeeksOfPeriod = new Set<string>();
    let date = dayjs(this.options.period.startAt);
    while (date.isBefore(endOfPeriod) || date.isSame(endOfPeriod)) {
      allWeeksOfPeriod.add(`${date.get('year')}-${date.week()}`);
      date = date.add(1, 'day');
    }
    return values.every(({ by }) => allWeeksOfPeriod.has(String(by)));
  }
}

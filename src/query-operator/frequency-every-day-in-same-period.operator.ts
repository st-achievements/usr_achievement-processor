import { usr } from '@st-achievements/database';
import dayjs from 'dayjs';
import { sql } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class FrequencyEveryDayInSamePeriodOperator extends QueryOperator {
  condition(): boolean {
    const { frequencyCondition, frequency, periodCondition } =
      this.options.achievement;
    return (
      frequencyCondition === 'every' &&
      frequency === 'day' &&
      periodCondition === 'samePeriod'
    );
  }

  execute(query: Query): Query {
    query.select.by = sql`${usr.workout.startedAt}::date`.mapWith(String);
    return query;
  }

  override isComplete(values: QueryResult[]): boolean {
    const endOfPeriod = dayjs(this.options.period.endAt);
    const allDaysOfPeriod = new Set<string>();
    let date = dayjs(this.options.period.startAt);
    while (date.isBefore(endOfPeriod) || date.isSame(endOfPeriod)) {
      allDaysOfPeriod.add(date.format('YYYY-MM-DD'));
      date = date.add(1, 'day');
    }
    return values.every(({ by }) => allDaysOfPeriod.has(String(by)));
  }
}

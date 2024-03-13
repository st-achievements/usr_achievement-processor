import { usr } from '@st-achievements/database';
import dayjs from 'dayjs';
import { sql } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class FrequencyEveryMonthInSamePeriodOperator extends QueryOperator {
  condition(): boolean {
    const { frequencyCondition, frequency, periodCondition } =
      this.options.achievement;
    return (
      frequencyCondition === 'every' &&
      frequency === 'month' &&
      periodCondition === 'samePeriod'
    );
  }

  execute(query: Query): Query {
    query.select.by = sql`to_char(${usr.workout.startedAt}, 'YYYY-MM')`;
    return query;
  }

  override isComplete(values: QueryResult[]): boolean {
    const endOfPeriod = dayjs(this.options.period.endAt);
    const allMonthsOfPeriod = new Set<string>();
    let date = dayjs(this.options.period.startAt);
    while (date.isBefore(endOfPeriod) || date.isSame(endOfPeriod)) {
      allMonthsOfPeriod.add(date.format('YYYY-MM'));
      date = date.add(1, 'day');
    }
    return values.every(({ by }) => allMonthsOfPeriod.has(String(by)));
  }
}
